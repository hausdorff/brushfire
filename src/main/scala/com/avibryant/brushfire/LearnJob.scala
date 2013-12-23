package com.avibryant.brushfire

import com.twitter.scalding._
import com.twitter.scalding.typed.TypedSink
import com.twitter.algebird._
import com.twitter.scalding.typed.{ValuePipe, ComputedValue, LiteralValue}

 class LearnJob(args : Args) extends Job(args) {
  implicit val splitSemigroup = new Semigroup[Split] {
    def plus(a : Split, b: Split) = if(a.p > b.p) a else b
  }

  def expandTree(
    trainingData: TypedPipe[(Map[String,Short], (Long,Long))],
    tree : ValuePipe[Tree]) : ValuePipe[Tree] = {
      val newTree =
        trainingData
          .flatMapWithValue(tree) {(data, treeOpt) =>
            val leaves = treeOpt.get.leaves
            val (row, stats) = data
            leaves
              .zipWithIndex
              .find{case (leaf, index) => leaf.includes(row)}
              .toList
              .flatMap {case (leaf, index) =>
                row.map {case (feature,value) =>
                    (index, feature) -> Map(value -> stats)
                }
              }
          }
          .group
          .sum
          .flatMap{case ((leafIndex, feature), valueStats) =>
            splits(feature, valueStats).map{split => Map(leafIndex -> split)}
          }
          .groupAll
          .sum
          .values
          .mapWithValue(tree){ (map, treeOpt) =>
            val leaves = treeOpt.get.leaves
            Tree(map.toList.flatMap{case (nodeIndex, split) => split.extend(leaves(nodeIndex))})
          }
        ComputedValue(newTree)
    }

    expandTree(readTrainingData, expandTree(readTrainingData, LiteralValue(EmptyTree)))
      .map{tree => tree.dump}
      .write(TypedSink(TextLine("/dev/null")))

  def splits(feature : String, stats : Map[Short,(Long,Long)]) : Iterable[Split]
    = {
      val total = Monoid.sum(stats.values)
      val statsList = stats.toList.sortBy{_._1}
      var acc = (0L,0L)

      statsList.map{case (value, stats) =>
        acc = Monoid.plus(acc, stats)
        Split(feature, value, acc, Group.minus(total, acc))
      }
    }

  def readTrainingData
    = TypedPipe.from(TextLine(args("input"))).map{line => parseTrainingData(line)}

  def parseTrainingData(line : String) = {
    val shorts = line.split("\t").map{_.toShort}.toList
    val stats = if(shorts.head == 1) (1L,0L) else (0L,1L)
    (Map(shorts.tail.zipWithIndex.map{case (v,i) => ("abcdefg"(i).toString, v)} : _*), stats)
  }
}

case class Tree(leaves : List[Node]) {
  def allNodes : Set[Node] = leaves.toSet.flatMap{n : Node => n.withAncestors}

  def dump {
    val childMap = Monoid.sum(allNodes.map{
      case Root => Map[Node,Set[Node]]()
      case n@SplitNode(p, _,_,_,_,_) => Map(p -> Set[Node](n))
    })
    dumpMap(childMap, Root, 0)
  }

  def dumpMap(map : Map[Node,Set[Node]], root : Node, indent : Int) {
    print(" " * indent)
    root match {
      case Root => println("Root")
      case SplitNode(_, f, v, gt, _, _) => {
        print(f)
        if(gt)
          print(" > ")
        else
          print(" <= ")
        println(v)
      }
    }
    map.getOrElse(root, Set[Node]()).foreach{c => dumpMap(map, c, indent+1)}
  }
}

object EmptyTree extends Tree(List(Root))

sealed trait Node {
  def includes(row : Map[String,Short]) : Boolean
  def withAncestors : Set[Node]
}

object Root extends Node {
  def includes(row : Map[String,Short]) = true
  def withAncestors = Set(this)
}

case class SplitNode(parent : Node,
                 feature : String,
                 value : Short,
                 greaterThan: Boolean,
                 goodCount : Long,
                 badCount : Long) extends Node {
  def includes(row : Map[String,Short]) = row(feature) > value == greaterThan && parent.includes(row)
  def withAncestors = parent.withAncestors + this
}

case class Split(feature : String, value : Short, lte: (Long,Long), gt: (Long,Long)) {

  def p = likelihoodRatio(lte._1, (lte._1 + lte._2), gt._1, (gt._1 + gt._2))

  def likelihoodRatio(k1 : Long, n1 : Long, k2 : Long, n2 : Long) = {
    def kLogP(k : Long, p : Double) = if(k == 0) 0 else k * math.log(p)
    def logL(p : Double, k : Long, n : Long) = kLogP(k, p) + kLogP(n - k, 1 - p)

    val p1 = k1.toDouble / n1.toDouble
    val p2 = k2.toDouble / n2.toDouble
    val p = (k1 + k2).toDouble / (n1 + n2).toDouble
    logL(p1, k1, n1) + logL(p2, k2, n2) - logL(p, k1, n1) - logL(p, k2, n2)
  }

  def extend(parent : Node) : List[Node] = {
    List(SplitNode(parent, feature, value, false, lte._1, lte._2),
         SplitNode(parent, feature, value, true, gt._1, gt._2))
  }
}