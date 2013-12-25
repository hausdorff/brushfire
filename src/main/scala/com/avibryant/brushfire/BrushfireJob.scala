package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.algebird._
import com.twitter.scalding.typed.{ValuePipe, ComputedValue, LiteralValue}

trait Learner[V,L,S,O] {
  def statsSemigroup : Semigroup[S]
  def predictionMonoid : Monoid[O]

  def buildStats(value : V, label : L) : S
  def findSplits(stats : S) : Iterable[Split[V,O]]
}

trait Scorer[L,O,C] {
  def scoreSemigroup : Semigroup[C]

  def scoreLabel(label : L, prediction: O) : C
}

case class Split[V,O](goodness : Double, predicates : Iterable[(V=>Boolean,O)])

trait BrushfireJob[K,V,L,S,O] extends Job {
  def learner : Learner[V,L,S,O]

  def expandTree(
    trainingData: TypedPipe[(Map[K,V],L)],
    tree : ValuePipe[Tree[K,V,O]])(implicit ko : Ordering[K])
     : ValuePipe[Tree[K,V,O]] = {

      implicit val ss = learner.statsSemigroup
      implicit val pm = learner.predictionMonoid
      implicit val splitSemigroup = Semigroup.from[(K,Split[V,O])]{(a,b) => if(a._2.goodness > b._2.goodness) a else b}

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
                    case Some((feature,split)) => split.predicates.map{
                      case (predicate,prediction) =>
                        val node = SplitNode(leaf, feature, predicate)
                        node -> prediction
                      }
                    case None => List(leaf -> prediction)
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
