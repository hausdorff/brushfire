package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.algebird._
import com.twitter.scalding.typed.{ValuePipe, ComputedValue, LiteralValue}

 trait BrushfireJob[K,V,L,S,P] extends Job {
  type MyTree = Tree[K,V]
  type MyNode = Node[K,V]
  type Row = Map[K,V]
  type LabeledRow = (Row,L)

  implicit def statsSemigroup : Semigroup[S]
  implicit def featureOrdering : Ordering[K]
  implicit def splitOrdering : Ordering[P]

  def buildStats(value : V, label : L) : S
  def findSplits(feature : K, stats : S) : Iterable[P]
  def extendTree(split : P, leaf : MyNode) : Iterable[MyNode]

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
                  yield (index, feature) -> buildStats(value, label)
          }
          .group
          .sum
          .flatMap{case ((index, feature), stats) =>
            findSplits(feature, stats).map{split => Map(index -> Max(split))}
          }
          .groupAll
          .sum
          .values
          .mapWithValue(tree){ (map, treeOpt) =>
            val newLeaves =
              for(tree <- treeOpt.toList;
                  (index,Max(split)) <- map.toList;
                  leaf <- extendTree(split, tree.leaves(index)))
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
