package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.algebird._
import com.twitter.scalding.typed.{ValuePipe, ComputedValue, LiteralValue}

trait Learner[K,V,L,S,P] {
  def statsSemigroup : Semigroup[S]
  def featureOrdering : Ordering[K]
  def splitOrdering : Ordering[P]

  def buildStats(value : V, label : L) : S
  def findSplits(feature : K, stats : S) : Iterable[P]
  def extendTree(split : P, leaf : Node[K,V]) : Iterable[Node[K,V]]
}

trait BrushfireJob[K,V,L,S,P] extends Job {
  type MyTree = Tree[K,V]
  type MyNode = Node[K,V]
  type Row = Map[K,V]
  type LabeledRow = (Row,L)

  def learner : Learner[K,V,L,S,P]

  implicit lazy val ss = learner.statsSemigroup
  implicit lazy val fo = learner.featureOrdering
  implicit lazy val so = learner.splitOrdering

  def expandTree(
    trainingData: TypedPipe[LabeledRow],
    tree : ValuePipe[MyTree]) : ValuePipe[MyTree] = {
      val newTree =
        trainingData
          .flatMapWithValue(tree) {(data, treeOpt) =>
            val (row, label) = data
            for(tree <- treeOpt.toList;
                index <- leafIndexFor(row, tree);
                (feature, value) <- row)
                  yield (index, feature) -> learner.buildStats(value, label)
          }
          .group
          .sum
          .flatMap{case ((index, feature), stats) =>
            learner.findSplits(feature, stats).map{split => Map(index -> Max(split))}
          }
          .groupAll
          .sum
          .values
          .mapWithValue(tree){ (map, treeOpt) =>
            val newLeaves =
              for(tree <- treeOpt.toList;
                  (index,Max(split)) <- map.toList;
                  leaf <- learner.extendTree(split, tree.leaves(index)))
                    yield leaf
            Tree(newLeaves)
          }
        ComputedValue(newTree)
    }

  def leafIndexFor(row : Row, tree : MyTree) : Iterable[Int] =
    tree
      .leaves
      .zipWithIndex
      .find{case (leaf, index) => leaf.includes(row)}
      .map{_._2}
      .toList
}
