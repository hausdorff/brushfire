package com.avibryant.brushfire

import com.twitter.scalding._

class TestJob(args : Args) extends Job(args) with BrushfireJob[String,Short,Boolean, Map[Short,(Long,Long)], (Long,Long)] {
  val depth = args.getOrElse("depth", "3").toInt

  lazy val learner = new BinaryLLRLearner[Short]

  val trainingData =
    TypedPipe
      .from(TextLine(args("input")))
      .map{line => parseTrainingData(line)}

  buildTreeNDeep(depth, trainingData)
    .map{tree => printTree(tree)}
    .write(TypedTsv[String](args("output")))

  def parseTrainingData(line : String) = {
    val shorts = line.split("\t").map{_.toShort}.toList
    val label = (shorts.head == 1)
    (Map(shorts.tail.zipWithIndex.map{case (v,i) => ("abcdefg"(i).toString, v)} : _*), label)
  }

  def printTree(tree : Tree[String,Short,(Long,Long)]) : String = {
    val sb = new StringBuilder

    tree.depthFirst{(level, node) =>
      sb ++= (" " * level)
      node match {
        case Root() => sb ++= ("root\n")
        case SplitNode(_, f, pred) => {
          sb ++= f
          sb ++= ": "
          sb ++= pred.toString
          sb ++= "\n"
        }
      }
    }

    sb.toString
  }
}
