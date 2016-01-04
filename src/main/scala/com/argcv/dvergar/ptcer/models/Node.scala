package com.argcv.dvergar.ptcer.models

import scala.collection.mutable.{ArrayBuffer, Map => MMap, Queue => MQueue, Set => MSet}

/**
  * @param nid pattern id
  * @param out neighbors out
  * @param in neighbors in
  * @param evq node queue
  */

case class Node(nid: Int,
                out: MSet[Int],
                in: MSet[Int],
                evq: MQueue[Event]) {

  /**
    * node garbage collection
    * remove node object,
    * return node ids (to remove in node nlacus)
    * @param cts current time stamp
    * @param delta delta
    * @return event ids to remove
    */
  def eventGC(cts: Long, delta: Long = 3): Array[Int] = evq.synchronized {
    val eids = ArrayBuffer[Int]()
    while (evq.headOption match {
      case Some(n) =>
        if (cts - n.ts > delta) {
          eids.append(evq.dequeue().eid)
          true
        } else {
          false
        }
      case None =>
        false
    }) ()
    eids.toArray
  }

  /**
    * get Nth event in this node
    * @param i index
    * @return
    */
  def eventGet(i: Int) = evq.get(i)

  /**
    * add a new event
    * @param e node object
    */
  def eventAdd(e: Event) = evq.synchronized(evq.enqueue(e))

  override def toString =
    s"[Node] node($nid), O(${out.mkString(",")})," +
      s" I(${in.mkString(",")}), @(${evq.map(_.eid).mkString(",")})"

}

object Node {
  def blank(pid: Int) = Node(pid, MSet[Int](), MSet[Int](), MQueue[Event]())
}
