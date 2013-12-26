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
  def allNodes = leaves.map{_._1}.toSet.flatMap{n : Node[K,V] => n.withAncestors}

  def depthFirst(fn : (Int,Node[K,V],O)=>Unit){
    val childMap = Monoid.sum(allNodes.map{
      case Root() => Map[Node[K,V],Set[Node[K,V]]]()
      case n : SplitNode[K,V] => Map(n.parent -> Set[Node[K,V]](n))
    })

    var predictionMap = Monoid.sum(leaves.map{
      case (node,prediction) =>
      Map(node.withAncestors.toList.map{n => (n,prediction)} : _*)
    })

    def depthFirstFrom(n : Node[K,V], level : Int) {
      fn(level, n, predictionMap(n))
      childMap.get(n).foreach{s => s.foreach{c => depthFirstFrom(c, level + 1)}}
    }

    depthFirstFrom(Root[K,V], 0)
  }
}

object Tree {
  def empty[K,V,O](implicit m : Monoid[O]) = Tree(List(Root[K,V] -> m.zero))
}
