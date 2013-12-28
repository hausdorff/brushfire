
package com.avibryant.brushfire.util

import com.avibryant.brushfire._
import com.twitter.algebird._

case class ConfusionMatrix[L](map: Map[(L, L), Long])

object ConfusionMatrix {
  def semigroup[L](implicit sg: Semigroup[Map[(L, L), Long]]) =
    Semigroup.from[ConfusionMatrix[L]] {
      case (ConfusionMatrix(a), ConfusionMatrix(b)) =>
        ConfusionMatrix(sg.plus(a, b))
    }

  def from[L](a: L, b: L) = ConfusionMatrix(Map((a, b) -> 1L))
}