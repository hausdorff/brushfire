package com.avibryant.brushfire.util

import com.twitter.algebird._

case class CrossTab[V, L](map: Map[(V, L), Long]) {
  def rowKeys = map.keys.map { _._1 }.toList.distinct
  def columnKeys = map.keys.map { _._2 }.toList.distinct
  def column(l: L) = map.collect { case ((v, ll), c) if l == ll => (v, c) }
  def row(v: V) = map.collect { case ((vv, l), c) if v == vv => (l, c) }
  def columnTotals = Map(columnKeys.map { l => l -> column(l).values.sum }: _*)
  def rowTotals = Map(rowKeys.map { v => v -> row(v).values.sum }: _*)
}

object CrossTab {
  def semigroup[V, L](implicit sg: Semigroup[Map[(V, L), Long]]) =
    Semigroup.from[CrossTab[V, L]] {
      case (CrossTab(a), CrossTab(b)) =>
        CrossTab(sg.plus(a, b))
    }

  def from[V, L](v: V, l: L) = CrossTab(Map((v, l) -> 1L))
}