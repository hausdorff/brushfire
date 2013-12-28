package com.avibryant.brushfire.util

import com.avibryant.brushfire._
import com.twitter.algebird._

/**
 * Defines a learner that aggregates log likelihood ratio statistics.
 *
 * @tparam V Type of incoming data.
 *
 * @param minInstances Heuristic specifying how many instances are required before
 * we're willing to partition the data.
 */
class BinaryLLRLearner[V](minInstances: Int)(implicit val ordering: Ordering[V])
    extends Learner[V, Boolean, CrossTab[V, Boolean], BooleanPrediction, ConfusionMatrix[Boolean]] {

  def predictionMonoid = BooleanPrediction.monoid
  def statsSemigroup = CrossTab.semigroup[V, Boolean]
  def errorSemigroup = ConfusionMatrix.semigroup[Boolean]

  /**
   * Produces a record that we've observed `value` with `label` once, so that we
   * can aggregate statistics later about how many times we've seen `value` with
   * `label`.
   *
   * The outcomes of `label` are represented as a CrossTab object: a matrix with
   * a row for each `value` and a column for each `label`, which maintains the counts
   * of how many times we've seen each combination.
   *
   * @param value The outcome of our observation.
   * @param label The label of our observation.
   *
   * @return CrossTab recording a count of 1 for (value,label) (these counts will be aggregated later).
   */
  def buildStats(value: V, label: Boolean) = {
    CrossTab.from(value, label)
  }

  /**
   * Produces (1) a series of predicates that map incoming data into partitions,
   * and (2) an evaluation of how good this partitioning scheme is.
   *
   * @param stats Aggregated statistics of how many times outcome v has been
   * observed with label l
   *
   * @return a BinarySplit on "<= v" for each value of v
   */
  def findSplits(stats: CrossTab[V, Boolean]): Iterable[Split[V, BooleanPrediction]] = {
    if (shouldSplit(stats)) {
      stats.rowKeys.map { v =>
        val lte = LessThanOrEqual(v)
        val gt = GreaterThan(v)
        val ct = stats.partition(lte)
        val llr = LogLikelihood.likelihoodRatio(ct)
        val ltePrediction = BooleanPrediction(ct.row(true))
        val gtPrediction = BooleanPrediction(ct.row(false))
        BinarySplit(llr, lte, ltePrediction, gt, gtPrediction)
      }
    } else {
      Some(EmptySplit[V, BooleanPrediction]())
    }
  }

  def findError(label: Boolean, prediction: BooleanPrediction) =
    ConfusionMatrix.from(label, prediction.bestGuess)

  def shouldSplit(stats: CrossTab[V, Boolean]) =
    stats.columnKeys.size > 1 &&
      stats.rowKeys.size > 1 &&
      stats.columnTotals.values.min >= minInstances
}
