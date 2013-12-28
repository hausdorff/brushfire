package com.avibryant.brushfire.util

import com.avibryant.brushfire._
import com.twitter.algebird._

class BinaryLLRLearner[V](minInstances: Int)(implicit val statsSemigroup: Semigroup[Map[V, (Long, Long)]],
  val predictionMonoid: Monoid[(Long, Long)],
  ordering: Ordering[V])
    extends Learner[V, Boolean, Map[V, (Long, Long)], (Long, Long), ConfusionMatrix[Boolean]] {

  def errorSemigroup = ConfusionMatrix.semigroup[Boolean]

  def shouldSplit(prediction: (Long, Long)) = prediction._1 >= minInstances && prediction._2 >= minInstances

  def buildStats(value: V, label: Boolean) = {
    Map(value -> (if (label) (1L, 0L) else (0L, 1L)))
  }

  def findSplits(stats: Map[V, (Long, Long)]) = {
    val total = Monoid.sum(stats.values)
    val statsList = stats.toList.sortBy { _._1 }
    var lte = (0L, 0L)

    if (shouldSplit(total)) {
      statsList.map {
        case (value, stats) =>
          lte = Monoid.plus(lte, stats)
          val gt = Group.minus(total, lte)
          val llr = LogLikelihood.likelihoodRatio(lte._1, (lte._1 + lte._2), gt._1, (gt._1 + gt._2))

          Split(llr, List(LessThanOrEqual(value) -> lte, GreaterThan(value) -> gt))
      }
    } else {
      List(Split(0.0, Nil))
    }
  }

  def findError(label: Boolean, prediction: (Long, Long)) =
    ConfusionMatrix.from(label, prediction._1 > prediction._2)
}