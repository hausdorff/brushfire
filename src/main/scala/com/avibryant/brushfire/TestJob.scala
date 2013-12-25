package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.twitter.algebird._
import com.twitter.scalding.typed.{ValuePipe, ComputedValue, LiteralValue}

class TestJob(args : Args) extends Job(args) with BrushfireJob[String,Short,Boolean, Map[Short,(Long,Long)], (Long,Long)] {

  lazy val learner = new TestLearner

  val trainingData =
    TypedPipe
      .from(TextLine(args("input")))
      .map{line => parseTrainingData(line)}

  expandTree(trainingData, expandTree(trainingData, LiteralValue(Tree.empty)))
    .map{tree => tree.dump}
    .write(TypedSink(TextLine("/dev/null")))

  def parseTrainingData(line : String) = {
    val shorts = line.split("\t").map{_.toShort}.toList
    val label = (shorts.head == 1)
    (Map(shorts.tail.zipWithIndex.map{case (v,i) => ("abcdefg"(i).toString, v)} : _*), label)
  }
}

class TestLearner(implicit
    val statsSemigroup : Semigroup[Map[Short,(Long,Long)]],
    val predictionMonoid : Monoid[(Long,Long)])
 extends Learner[Short, Boolean, Map[Short,(Long,Long)], (Long,Long)]{

  def buildStats(value : Short, label : Boolean) = {
    Map(value -> (if(label) (1L,0L) else (0L,1L)))
  }

  def findSplits(stats : Map[Short,(Long,Long)]) = {
    val total = Monoid.sum(stats.values)
    val statsList = stats.toList.sortBy{_._1}
    var lte = (0L,0L)

    statsList.map{case (value, stats) =>
      lte = Monoid.plus(lte, stats)
      val gt = Group.minus(total, lte)

      object LTE extends Function1[Short,Boolean] {
        def apply(v : Short) = v <= value
        override def toString = "<= " + value.toString
      }

      object GT extends Function1[Short,Boolean] {
        def apply(v : Short) = v > value
        override def toString = "> " + value.toString
      }

      val llr = likelihoodRatio(lte._1, (lte._1 + lte._2), gt._1, (gt._1 + gt._2))

      Split(llr, List(LTE -> lte,GT -> gt))
    }
  }

  def likelihoodRatio(k1 : Long, n1 : Long, k2 : Long, n2 : Long) = {
    def kLogP(k : Long, p : Double) = if(k == 0) 0 else k * math.log(p)
    def logL(p : Double, k : Long, n : Long) = kLogP(k, p) + kLogP(n - k, 1 - p)

    val p1 = k1.toDouble / n1.toDouble
    val p2 = k2.toDouble / n2.toDouble
    val p = (k1 + k2).toDouble / (n1 + n2).toDouble
    logL(p1, k1, n1) + logL(p2, k2, n2) - logL(p, k1, n1) - logL(p, k2, n2)
  }
}