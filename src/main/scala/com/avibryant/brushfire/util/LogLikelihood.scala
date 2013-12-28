package com.avibryant.brushfire.util

object LogLikelihood {

  def likelihoodRatio(crossTab: CrossTab[Boolean, Boolean]): Double = {
    val totals = crossTab.rowTotals
    val goods = crossTab.column(true)
    likelihoodRatio(
      goods.getOrElse(true, 0L),
      totals.getOrElse(true, 0L),
      goods.getOrElse(false, 0L),
      totals.getOrElse(false, 0L))
  }

  def likelihoodRatio[N](k1: N, n1: N, k2: N, n2: N)(implicit n: Numeric[N]): Double = {

    val kd1 = n.toDouble(k1)
    val kd2 = n.toDouble(k2)
    val nd1 = n.toDouble(n1)
    val nd2 = n.toDouble(n2)

    val p1 = kd1 / nd1
    val p2 = kd2 / nd2
    val p = (kd1 + kd2) / (nd1 + nd2)
    logL(p1, kd1, nd1) + logL(p2, kd2, nd2) - logL(p, kd1, nd1) - logL(p, kd2, nd2)
  }

  def kLogP(k: Double, p: Double) = if (k == 0.0) 0.0 else k * math.log(p)
  def logL(p: Double, k: Double, n: Double) = kLogP(k, p) + kLogP(n - k, 1.0 - p)
}