package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.twitter.algebird._
import com.twitter.scalding.typed.{ValuePipe, ComputedValue, LiteralValue}

class TestJob(args : Args) extends Job(args) with BrushfireJob[String,Short,Boolean, Map[Short,(Long,Long)], (Long,Long)] {

  lazy val learner = new BinaryLLRLearner[Short]

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
