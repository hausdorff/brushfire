package com.avibryant.brushfire.util

case class LessThanOrEqual[V](value: V)(implicit ord: Ordering[V])
    extends Function1[V, Boolean] {
  def apply(v: V) = ord.lteq(v, value)
  override def toString = "<= " + value.toString
}

case class GreaterThan[V](value: V)(implicit ord: Ordering[V])
    extends Function1[V, Boolean] {
  def apply(v: V) = ord.gt(v, value)
  override def toString = "> " + value.toString
}