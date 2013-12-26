package com.avibryant.brushfire
import com.twitter.algebird._

class BinaryLLRLearner[V](implicit
    val statsSemigroup : Semigroup[Map[V,(Long,Long)]],
    val predictionMonoid : Monoid[(Long,Long)],
    ordering : Ordering[V])
 extends Learner[V, Boolean, Map[V,(Long,Long)], (Long,Long)]{

  def buildStats(value : V, label : Boolean) = {
    Map(value -> (if(label) (1L,0L) else (0L,1L)))
  }

  def findSplits(stats : Map[V,(Long,Long)]) = {
    val total = Monoid.sum(stats.values)
    val statsList = stats.toList.sortBy{_._1}
    var lte = (0L,0L)

    if(total._1 > 0 && total._2 > 0) {
      statsList.map{case (value, stats) =>
        lte = Monoid.plus(lte, stats)
        val gt = Group.minus(total, lte)

        object LTE extends Function1[V,Boolean] {
          def apply(v : V) = ordering.lteq(v, value)
          override def toString = "<= " + value.toString
        }

        object GT extends Function1[V,Boolean] {
          def apply(v : V) = ordering.gt(v, value)
          override def toString = "> " + value.toString
        }

        val llr = likelihoodRatio(lte._1, (lte._1 + lte._2), gt._1, (gt._1 + gt._2))

        Split(llr, List(LTE -> lte,GT -> gt))
      }
    } else {
      List(Split(0.0, Nil))
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