package com.argcv.dvergar.ptcer.models

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{ Set => MSet }

case class TrieNode[V](base: Option[Edge], // current node
  next: TrieMap[Edge, TrieNode[V]],
  v: MSet[V] = MSet[V]()) {
}

class TrieTree[V] {
  val root: TrieNode[V] = new TrieNode[V](None, TrieMap[Edge, TrieNode[V]]())

  val insertMonitor = new AnyRef

  /**
   * @param path edge sequence
   * @param v value of this sequence
   * @return
   */
  def put(path: List[Edge], v: V): Unit = insertMonitor.synchronized {
    var p = root
    for (c <- path) {
      if (!p.next.contains(c)) {
        val t = c -> TrieNode[V](Some(c), TrieMap[Edge, TrieNode[V]]())
        p.next += t
      }
      p = p.next.get(c).get
    }
    p.v += v
  }

  def check(path: List[Edge]): (Option[V], Boolean) = {
    if (path.nonEmpty) {
      val c2l = path.drop(1)
      var rp: Option[TrieNode[V]] = root.next.get(path.head)
      def forward(): Unit =
        for (c <- c2l) {
          rp = rp match {
            case Some(gp) => gp.next.get(c)
            case None => return
          }
        }
      forward()
      rp match {
        case Some(n) => (n.v.headOption, true)
        case None => (None, false)
      }
    } else {
      (None, false)
    }
  }
}

object TrieTree {
  def apply[V](): TrieTree[V] = {
    new TrieTree[V]()
  }
}