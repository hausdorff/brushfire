package com.avibryant.brushfire.example

import com.avibryant.brushfire._
import com.twitter.scalding._

class IrisJob(args : Args) extends Job(args) with BrushfireJob[String,Short,Boolean, Map[Short,(Long,Long)], (Long,Long)] {
  val depth = args.getOrElse("depth", "3").toInt
  val folds = args.getOrElse("folds", "2").toInt
  val target = args.required("target")

  lazy val learner = new BinaryLLRLearner[Short]

  val trainingData =
    TypedPipe
      .from(TextLine(args("input")))
      .map{line => parseTrainingData(line)}

  buildTreesToDepth(depth, folds, trainingData)
    .map{case (fold,tree) => "Fold " + fold.toString + "\n" + printTree(tree)}
    .write(TypedTsv[String](args("output")))

  val cols = List("petal-width", "petal-length", "sepal-width", "sepal-length")
  def parseTrainingData(line : String) = {
    val parts = line.split(",").reverse.toList
    val label = parts.head == target
    val shorts = parts.tail.map{s => (s.toDouble * 10).toShort}
    (Map(cols.zip(shorts) : _*), label)
  }

  def printTree(tree : Tree[String,Short,(Long,Long)]) : String = {
    val sb = new StringBuilder

    tree.depthFirst{(level, node, prediction) =>
      sb ++= (" " * level)
      node match {
        case Root() => sb ++= ("root")
        case SplitNode(_, f, pred) => {
          sb ++= f
          sb ++= ": "
          sb ++= pred.toString
          sb ++= "mm"
        }
      }

      val (good,bad) = prediction
      sb ++= " (" + good.toString + "," + bad.toString + ")"
      sb ++= "\n"
    }

    sb.toString
  }
}
