package com.avibryant.brushfire

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

case class Tree[K,V](leaves : List[Node[K,V]]) {
  def allNodes = leaves.toSet.flatMap{n : Node[K,V] => n.withAncestors}

  def dump {
    val childMap = com.twitter.algebird.Monoid.sum(allNodes.map{
      case Root() => Map[Node[K,V],Set[Node[K,V]]]()
      case n : SplitNode[K,V] => Map(n.parent -> Set[Node[K,V]](n))
    })
    dumpMap(childMap, Root[K,V], 0)
  }

  def dumpMap(map : Map[Node[K,V],Set[Node[K,V]]], root : Node[K,V], indent : Int) {
    print(" " * indent)
    root match {
      case Root() => println("root")
      case SplitNode(_, f, pred) => {
        print(f)
        print(": ")
        println(pred)
      }
    }
    map.getOrElse(root, Set[Node[K,V]]()).foreach{c => dumpMap(map, c, indent+1)}
  }
}

object Tree {
  def empty[K,V] = Tree(List(Root[K,V]))
}
