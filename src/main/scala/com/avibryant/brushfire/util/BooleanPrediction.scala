package com.avibryant.brushfire.util

import com.twitter.algebird._

case class BooleanPrediction(trueFrequency: Long, falseFrequency: Long) {
  def bestGuess = trueFrequency > falseFrequency
}

object BooleanPrediction {
  val monoid = Monoid.from(BooleanPrediction(0L, 0L)) {
    (a, b) => BooleanPrediction(a.trueFrequency + b.trueFrequency, a.falseFrequency + b.falseFrequency)
  }

  def apply(map: Map[Boolean, Long]): BooleanPrediction =
    BooleanPrediction(map.getOrElse(true, 0L), map.getOrElse(false, 0L))
}