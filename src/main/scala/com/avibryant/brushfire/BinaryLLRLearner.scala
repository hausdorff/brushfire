package com.avibryant.brushfire
import com.twitter.algebird._

/** Defines a learner that aggregates log likelihood ratio statistics.
  * 
  * @tparam V Type of incoming data.
  * 
  * @param statsSemigroup Semigroup used to aggregate the log likelihood ratio of
  * observing a feature v with label l.
  * @param predictionMonoid 
  */
class BinaryLLRLearner[V](implicit
    val statsSemigroup : Semigroup[Map[V,(Long,Long)]],
    val predictionMonoid : Monoid[(Long,Long)],
    ordering : Ordering[V])
 extends Learner[V, Boolean, Map[V,(Long,Long)], (Long,Long)]{

  /** Produces a record that we've observed `value` with `label` once, so that we
    * can aggregate statistics later about how many times we've seen `value` with
    * `label`.
    *
    * The outcomes of `label` are represented as a vector of counts: if `label` is
    * true, we produce a Map(value -> (1,0)), while if it's false, we produce
    * Map(value -> (0,1)).
    *
    * @param value The outcome of our observation.
    * @param label The label of our observation.
    * 
    * @return Map value -> vector indicating we've observed it occurring with
    * `label` once (these counts will be aggregated later).
    */
  def buildStats(value : V, label : Boolean) = {
    Map(value -> (if(label) (1L,0L) else (0L,1L)))
  }

  /** Produces (1) a series of predicates that map incoming data into partitions,
    * and (2) an evaluation of how good this partitioning scheme is.
    *
    * @param stats Aggregated statistics of how many times outcome v has been
    * observed with label l
    *
    * @return A Split representing how data are partitioned, and an evaluation
    * of how good that split is.
    */
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

  //
  // ML/Learning functions
  //

  def likelihoodRatio(k1 : Long, n1 : Long, k2 : Long, n2 : Long) = {
    def kLogP(k : Long, p : Double) = if(k == 0) 0 else k * math.log(p)
    def logL(p : Double, k : Long, n : Long) = kLogP(k, p) + kLogP(n - k, 1 - p)

    val p1 = k1.toDouble / n1.toDouble
    val p2 = k2.toDouble / n2.toDouble
    val p = (k1 + k2).toDouble / (n1 + n2).toDouble
    logL(p1, k1, n1) + logL(p2, k2, n2) - logL(p, k1, n1) - logL(p, k2, n2)
  }
}
