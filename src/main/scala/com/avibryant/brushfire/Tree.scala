package com.avibryant.brushfire

import com.twitter.algebird._

sealed trait Node[K,V] {
  def includes(row : Map[K,V]) : Boolean
  def withAncestors : Set[Node[K,V]]
}

case class Root[K,V] extends Node[K,V] {
  def includes(row : Map[K,V]) = true
  def withAncestors = Set(this)
}

case class SplitNode[K,V]
  (parent : Node[K,V],
   feature : K,
   predicate : V => Boolean)
    extends Node[K,V] {
      def includes(row : Map[K,V]) =
        predicate(row(feature)) && parent.includes(row)

      def withAncestors = parent.withAncestors + this
}

case class Tree[K,V,O](leaves : Seq[(Node[K,V],O)])(implicit m : Monoid[O]) {
  type N = Node[K,V]

  def allNodes = leaves.map{_._1}.toSet.flatMap{n : N => n.withAncestors}

  def dump {
    val childMap = com.twitter.algebird.Monoid.sum(allNodes.map{
      case Root() => Map[N,Set[N]]()
      case n : SplitNode[K,V] => Map(n.parent -> Set[N](n))
    })
    dumpMap(childMap, Root[K,V], 0)
  }

  def dumpMap(map : Map[N,Set[N]], root : N, indent : Int) {
    print(" " * indent)
    root match {
      case Root() => println("root")
      case SplitNode(_, f, pred) => {
        print(f)
        print(": ")
        println(pred)
      }
    }
    map.getOrElse(root, Set[N]()).foreach{c => dumpMap(map, c, indent+1)}
  }
}

object Tree {
  def empty[K,V,O](implicit m : Monoid[O]) = Tree(List(Root[K,V] -> m.zero))
}
