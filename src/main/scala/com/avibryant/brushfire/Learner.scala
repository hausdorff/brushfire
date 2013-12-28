package com.avibryant.brushfire

import com.twitter.algebird._

/**
 * `Learner` provides the core logic for learning a decision tree. A `Learner`
 * aggregates statistics about features, and uses these statistics to (1) propose
 * the segmentations that make up a decision tree, and (2) produce an evaluation
 * of the of the segmentation (e.g., a p-value).
 *
 * This process generally has two parts. First, aggregation requires mapping
 * observations to members of a semigroup, which will allow us to aggregate
 * statistics about the observations. This logic is defined in `buildStats`; see
 * example learners for how this is usually done.
 *
 * The other element is to define `findSplits`, which we will define Splits
 * of the data (which includes both a partition for the data, and an evaluation
 * of that partition).
 *
 * @tparam V Type of incoming data.
 * @tparam L Type of label given to data.
 * @tparam S Type of statistic aggregated (e.g., a word count).
 * @tparam O Type of output (i.e., prediction).
 * @tparam E Type of error aggregated (e.g., a confusion matrix).
 */
trait Learner[V, L, S, O, E] {
  def statsSemigroup: Semigroup[S]
  def predictionMonoid: Monoid[O]
  def errorSemigroup: Semigroup[E]

  def buildStats(value: V, label: L): S
  def findSplits(stats: S): Iterable[Split[V, O]]
  def findError(label: L, prediction: O): E
}

/**
 * A `Split` provides a scheme for partitioning your data (via `predicates`)
 * and a score for how "good" that partitioning is (via `goodness` which is, e.g.,
 * a p-value) according to some `Learner`.
 *
 * @tparam V Type of incoming data.
 * @tparam O Type of output (i.e., prediction).
 *
 */
sealed trait Split[V, O] {
  def goodness: Double
  def predicates: Iterable[(V => Boolean, O)]
}

case class EmptySplit[V, O] extends Split[V, O] {
  val goodness = 0.0
  val predicates = Nil
}

case class BinarySplit[V, O](val goodness: Double,
  leftPredicate: V => Boolean,
  leftPrediction: O,
  rightPredicate: V => Boolean,
  rightPrediction: O)
    extends Split[V, O] {
  def predicates = List(leftPredicate -> leftPrediction, rightPredicate -> rightPrediction)
}