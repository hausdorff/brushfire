package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.algebird._
import com.twitter.scalding.typed.{ ValuePipe, ComputedValue, LiteralValue }

trait BrushfireJob[K, V, L, S, O, C] extends Job {
  def learner: Learner[V, L, S, O]
  def scorer: Scorer[L, O, C]

  def learn(
    depth: Int,
    folds: Int,
    trainingData: TypedPipe[(Map[K, V], L)])(implicit ko: Ordering[K]) = {
    implicit val pm = learner.predictionMonoid
    lazy val rand = new scala.util.Random
    val withFolds = trainingData.map { case (row, label) => (rand.nextInt(folds), row, label) }
    val emptyTrees = TypedPipe.from((0.to(folds - 1).toList.map { (_, Tree.empty[K, V, O]) }))
    val fullTrees = expandTreesToDepth(depth, withFolds, emptyTrees)
    (fullTrees, scoreTrees(withFolds, fullTrees))
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

  def scoreTrees(
    trainingData: TypedPipe[(Int, Map[K, V], L)],
    trees: TypedPipe[(Int, Tree[K, V, O])]): ValuePipe[C] = {
    implicit val ss = scorer.scoreSemigroup

    trainingData
      .groupBy { _._1 }
      .hashJoin(trees.group)
      .values
      .flatMap {
        case ((treeFold, row, label), tree) =>
          tree
            .leaves
            .find { _._1.includes(row) }
            .map { case (leaf, prediction) => scorer.scoreLabel(label, prediction) }
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
