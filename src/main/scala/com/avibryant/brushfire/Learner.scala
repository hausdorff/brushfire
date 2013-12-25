package com.avibryant.brushfire

import com.twitter.algebird._

trait Learner[V,L,S,O] {
  def statsSemigroup : Semigroup[S]
  def predictionMonoid : Monoid[O]

  def buildStats(value : V, label : L) : S
  def findSplits(stats : S) : Iterable[Split[V,O]]
}

case class Split[V,O](goodness : Double, predicates : Iterable[(V=>Boolean,O)])

trait Scorer[L,O,C] {
  def scoreSemigroup : Semigroup[C]

  def scoreLabel(label : L, prediction: O) : C
}
