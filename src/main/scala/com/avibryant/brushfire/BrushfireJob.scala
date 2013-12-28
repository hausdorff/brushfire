package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.algebird._

/** General template for a fully-specified Brushfire job.
  *
  * @tparam K Defines ordering for features in the model.
  * @tparam V Type of incoming data.
  * @tparam L Type of label given to data.
  * @tparam S Type of statistic aggregated (e.g., an approximation of the joint
  * distribution).
  * @tparam O Type of output (i.e., prediction).
  * @tparam E Type of error aggregated (e.g., a confusion matrix).
  */
trait BrushfireJob[K, V, L, S, O, E] extends Job {
  def learner: Learner[V, L, S, O, E]

  def learn(
    depth: Int,
    folds: Int,
    trainingData: TypedPipe[(Map[K, V], L)])(implicit ko: Ordering[K]) = {
    implicit val pm = learner.predictionMonoid
    lazy val rand = new scala.util.Random
    val withFolds = trainingData.map { case (row, label) => (rand.nextInt(folds), row, label) }
    val emptyTrees = TypedPipe.from((0.to(folds - 1).toList.map { (_, Tree.empty[K, V, O]) }))
    val fullTrees = expandTreesToDepth(depth, withFolds, emptyTrees)
    (fullTrees, evaluateTrees(withFolds, fullTrees))
  }

  def expandTreesToDepth(
    depth: Int,
    trainingData: TypedPipe[(Int, Map[K, V], L)],
    trees: TypedPipe[(Int, Tree[K, V, O])])(implicit ko: Ordering[K]): TypedPipe[(Int, Tree[K, V, O])] = {
    if (depth > 0)
      expandTreesToDepth(depth - 1, trainingData, expandTrees(trainingData, trees))
    else
      trees
  }

  def expandTrees(trainingData: TypedPipe[(Int, Map[K, V], L)],
    trees: TypedPipe[(Int, Tree[K, V, O])])(implicit ko: Ordering[K]) = {

    implicit val ss = learner.statsSemigroup
    implicit val splitSemigroup = Semigroup.from[(K, Split[V, O])] { (a, b) => if (a._2.goodness > b._2.goodness) a else b }
    implicit val pm = learner.predictionMonoid

    trainingData
      .cross(trees)
      .flatMap {
        case ((testFold, row, label), (treeFold, tree)) =>
          for (
            index <- leafIndexFor(row, tree) if testFold != treeFold;
            (feature, value) <- row
          ) yield (treeFold, index, feature) -> learner.buildStats(value, label)
      }
      .group
      .sum
      .flatMap {
        case ((treeFold, index, feature), stats) =>
          learner.findSplits(stats).map { split => treeFold -> Map(index -> (feature, split)) }
      }
      .group
      .sum
      .group
      .hashJoin(trees.group)
      .map {
        case (treeFold, (map, tree)) =>
          val newLeaves =
            tree
              .leaves
              .zipWithIndex
              .flatMap {
                case ((leaf, prediction), index) =>
                  map.get(index) match {
                    case None => List(leaf -> prediction)
                    case Some((feature, Split(_, Nil))) => List(leaf -> prediction)
                    case Some((feature, Split(_, predicates))) => predicates.map {
                      case (predicate, prediction) =>
                        val node = SplitNode(leaf, feature, predicate)
                        node -> prediction
                    }
                  }
              }

          treeFold -> Tree(newLeaves)
      }
  }

  def evaluateTrees(
    trainingData: TypedPipe[(Int, Map[K, V], L)],
    trees: TypedPipe[(Int, Tree[K, V, O])]) = {
    implicit val es = learner.errorSemigroup

    trainingData
      .groupBy { _._1 }
      .hashJoin(trees.group)
      .values
      .flatMap {
        case ((treeFold, row, label), tree) =>
          tree
            .leaves
            .find { _._1.includes(row) }
            .map { case (leaf, prediction) => learner.findError(label, prediction) }
      }
      .sum
  }

  def leafIndexFor(row: Map[K, V], tree: Tree[K, V, O]): Iterable[Int] =
    tree
      .leaves
      .zipWithIndex
      .find { case ((leaf, prediction), index) => leaf.includes(row) }
      .map { _._2 }
      .toList
}
