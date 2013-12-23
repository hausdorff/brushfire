package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.twitter.algebird._
import com.twitter.scalding.typed.{ValuePipe, ComputedValue, LiteralValue}

class TestJob(args : Args) extends Job(args)
  with BrushfireJob[String,Short,
                    (Long,Long),
                    Map[Short,(Long,Long)],
                    Split]
  {
  val trainingData =
    TypedPipe
      .from(TextLine(args("input")))
      .map{line => parseTrainingData(line)}

  expandTree(trainingData, expandTree(trainingData, LiteralValue(Tree.empty)))
    .map{tree => tree.dump}
    .write(TypedSink(TextLine("/dev/null")))

  implicit lazy val statsSemigroup = Semigroup.mapSemigroup[Short,(Long,Long)]
  implicit lazy val splitOrdering = Ordering.by[Split,Double]{_.llr}
  implicit lazy val featureOrdering = Ordering.by[String,String](identity)

  def buildStats(value : Short, label : (Long,Long)) = {
    Map(value -> label)
  }

  def findSplits(feature : String, stats : Map[Short,(Long,Long)]) : Iterable[Split]
    = {
      val total = Monoid.sum(stats.values)
      val statsList = stats.toList.sortBy{_._1}
      var acc = (0L,0L)

      statsList.map{case (value, stats) =>
        acc = Monoid.plus(acc, stats)
        Split(feature, value, acc, Group.minus(total, acc))
      }
    }

  def extendTree(split : Split, leaf : Node[String,Short]) = split.extend(leaf)

  def parseTrainingData(line : String) = {
    val shorts = line.split("\t").map{_.toShort}.toList
    val label = if(shorts.head == 1) (1L,0L) else (0L,1L)
    (Map(shorts.tail.zipWithIndex.map{case (v,i) => ("abcdefg"(i).toString, v)} : _*), label)
  }
}

case class Split(feature : String, value : Short, lte: (Long,Long), gt: (Long,Long)) {

  def llr = likelihoodRatio(lte._1, (lte._1 + lte._2), gt._1, (gt._1 + gt._2))

  def likelihoodRatio(k1 : Long, n1 : Long, k2 : Long, n2 : Long) = {
    def kLogP(k : Long, p : Double) = if(k == 0) 0 else k * math.log(p)
    def logL(p : Double, k : Long, n : Long) = kLogP(k, p) + kLogP(n - k, 1 - p)

    val p1 = k1.toDouble / n1.toDouble
    val p2 = k2.toDouble / n2.toDouble
    val p = (k1 + k2).toDouble / (n1 + n2).toDouble
    logL(p1, k1, n1) + logL(p2, k2, n2) - logL(p, k1, n1) - logL(p, k2, n2)
  }

  object LTE extends Function1[Short,Boolean] {
    def apply(v : Short) = v <= value
    override def toString = "<= " + value.toString
  }

  object GT extends Function1[Short,Boolean] {
    def apply(v : Short) = v > value
    override def toString = "> " + value.toString
  }

  def extend(parent : Node[String,Short]) : List[Node[String,Short]] = {
    List(SplitNode[String,Short](parent, feature, LTE),
         SplitNode[String,Short](parent, feature, GT))
  }
}