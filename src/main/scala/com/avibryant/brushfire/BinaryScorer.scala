
package com.avibryant.brushfire
import com.twitter.algebird._

case class BinaryScore(
  truePositives: Long = 0,
  trueNegatives: Long = 0,
  falsePositives: Long = 0,
  falseNegatives: Long = 0)

class BinaryScorer
    extends Scorer[Boolean, (Long, Long), BinaryScore] {

  val scoreSemigroup = Semigroup.from[BinaryScore] { (a, b) =>
    BinaryScore(
      a.truePositives + b.truePositives,
      a.trueNegatives + b.trueNegatives,
      a.falsePositives + b.falsePositives,
      a.falseNegatives + b.falseNegatives)
  }

  def scoreLabel(label: Boolean, prediction: (Long, Long)): BinaryScore = {
    (label, prediction._1 > prediction._2) match {
      case (true, true) => BinaryScore(truePositives = 1)
      case (true, false) => BinaryScore(falseNegatives = 1)
      case (false, false) => BinaryScore(trueNegatives = 1)
      case (false, true) => BinaryScore(falsePositives = 1)
    }
  }
}