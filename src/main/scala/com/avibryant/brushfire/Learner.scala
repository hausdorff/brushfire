package com.avibryant.brushfire

import com.twitter.algebird._

trait Learner[V, L, S, O, E] {
  def statsSemigroup: Semigroup[S]
  def predictionMonoid: Monoid[O]
  def errorSemigroup: Semigroup[E]

  def shouldSplit(prediction: O): Boolean
  def buildStats(value: V, label: L): S
  def findSplits(stats: S): Iterable[Split[V, O]]
  def findError(label: L, prediction: O): E
}

case class Split[V, O](goodness: Double, predicates: Iterable[(V => Boolean, O)])