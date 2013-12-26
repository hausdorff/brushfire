package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.algebird._
import com.twitter.scalding.typed.{ValuePipe, ComputedValue, LiteralValue}

trait BrushfireJob[K,V,L,S,O] extends Job {
  def learner : Learner[V,L,S,O]

  def buildTreeNDeep(
    n : Int,
    trainingData : TypedPipe[(Map[K,V],L)])(implicit ko : Ordering[K])
     = {
      implicit val pm = learner.predictionMonoid
      expandTreeNTimes(n, trainingData, LiteralValue(Tree.empty))
  }

  def expandTreeNTimes(
    n : Int,
    trainingData : TypedPipe[(Map[K,V],L)],
    tree : ValuePipe[Tree[K,V,O]])(implicit ko : Ordering[K]) : ValuePipe[Tree[K,V,O]]
     = {

      if(n > 0)
        expandTreeNTimes(n - 1, trainingData, expandTree(trainingData, tree))
      else
        tree
  }

  def expandTree(
    trainingData : TypedPipe[(Map[K,V],L)],
    tree : ValuePipe[Tree[K,V,O]])(implicit ko : Ordering[K])
      = {

      implicit val ss = learner.statsSemigroup
      implicit val splitSemigroup = Semigroup.from[(K,Split[V,O])]{(a,b) => if(a._2.goodness > b._2.goodness) a else b}
      implicit val pm = learner.predictionMonoid

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
            learner.findSplits(stats).map{split => Map(index -> (feature,split))}
          }
          .groupAll
          .sum
          .values
          .mapWithValue(tree){ (map, treeOpt) =>
            val newLeaves =
              treeOpt
                .get
                .leaves
                .zipWithIndex
                .flatMap{case((leaf,prediction),index) =>
                  map.get(index) match {
                    case None => List(leaf -> prediction)
                    case Some((feature,Split(_,Nil))) => List(leaf -> prediction)
                    case Some((feature,Split(_,predicates))) => predicates.map{
                      case (predicate,prediction) =>
                        val node = SplitNode(leaf, feature, predicate)
                        node -> prediction
                    }
                  }
                }

            Tree(newLeaves)
          }
        ComputedValue(newTree)
    }

  def leafIndexFor(row : Map[K,V], tree : Tree[K,V,O]) : Iterable[Int] =
    tree
      .leaves
      .zipWithIndex
      .find{case ((leaf,prediction), index) => leaf.includes(row)}
      .map{_._2}
      .toList
}
