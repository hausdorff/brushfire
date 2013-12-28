package com.avibryant.brushfire.util

import com.avibryant.brushfire._
import com.twitter.algebird._

/** Defines a learner that aggregates log likelihood ratio statistics.
  * 
  * @tparam V Type of incoming data.
  * 
  * @param minInstances Heuristic specifying how many instances are required before
  * we're willing to partition the data.
  * @param statsSemigroup Semigroup used to aggregate the log likelihood ratio of
  * observing a feature v with label l.
  * @param predictionMonoid 
  */
class BinaryLLRLearner[V](minInstances: Int)(implicit val statsSemigroup: Semigroup[Map[V, (Long, Long)]],
  val predictionMonoid: Monoid[(Long, Long)],
  ordering: Ordering[V])
    extends Learner[V, Boolean, Map[V, (Long, Long)], (Long, Long), ConfusionMatrix[Boolean]] {

  def errorSemigroup = ConfusionMatrix.semigroup[Boolean]

  def shouldSplit(prediction: (Long, Long)) = prediction._1 >= minInstances && prediction._2 >= minInstances

  /** Produces a record that we've observed `value` with `label` once, so that we
    * can aggregate statistics later about how many times we've seen `value` with
    * `label`.
    *
    * The outcomes of `label` are represented as a vector of counts: if `label` is
    * true, we produce a Map(value -> (1,0)), while if it's false, we produce
    * Map(value -> (0,1)).
    *
    * @param value The outcome of our observation.
    * @param label The label of our observation.
    * 
    * @return Map value -> vector indicating we've observed it occurring with
    * `label` once (these counts will be aggregated later).
    */
  def buildStats(value: V, label: Boolean) = {
    Map(value -> (if (label) (1L, 0L) else (0L, 1L)))
  }

  /** Produces (1) a series of predicates that map incoming data into partitions,
    * and (2) an evaluation of how good this partitioning scheme is.
    *
    * @param stats Aggregated statistics of how many times outcome v has been
    * observed with label l
    *
    * @return A Split representing how data are partitioned, and an evaluation
    * of how good that split is.
    */
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
