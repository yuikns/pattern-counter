package com.argcv.dvergar.ptcer.models

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}

/**
  * @author yu
  * @param nsize node size
  */
class Graph(nsize: Int) {
  /**
    * generate global event id for this graph
    */
  lazy val eidGen: AtomicInteger = new AtomicInteger

  /**
    * event lacus
    */
  lazy val nlacus: Array[Node] = (0 until nsize).map(i => Node.blank(i)).toArray

  /**
    * node map, <br/>
    * key: Event.eid
    * value: Event instance
    */
  lazy val emap: MMap[Int, Event] = MMap[Int, Event]()
  //lazy val acMap = TrieMap[String, (Option[Int], Boolean)]()

  /**
    * add a new node
    * @param nid pattern id
    * @param ts timestamp
    * @return new event id
    */
  def eventAdd(nid: Int, ts: Long): Int = {
    val eid = eidGen.getAndIncrement()
    val node = new Event(eid = eid, nid = nid, ts = ts)
    emap.synchronized(emap.put(eid, node))
    nlacus(nid).eventAdd(node)
    eid
  }

  override def toString =
    (nlacus.map(_.toString).toList ::: emap.values.map(_.toString).toList).mkString("\n")

  /**
    * node garbage collection
    * @param cts current time stamp
    * @param delta delta
    * @return node id to remove
    */
  def eventGC(cts: Long, delta: Long = 3): Unit = {
    //System.out.println(s"start gc $cts")
    val eids = nlacus.par.flatMap(_.eventGC(cts, delta)).toArray
    eventRm(eids)
    //System.out.println(s"end gc $cts , size: ${eids.length}")
  }

  /**
    * event remove from event nlacus
    * @param eids node ids
    */
  def eventRm(eids: Array[Int]) = emap.synchronized(eids.foreach(emap.remove))

  /**
    * @param eid event id to event id out
    * @return
    */
  def eid2EidOut(eid: Int): List[Int] = eid2EOut(eid).map(_.eid)

  /**
    * @param eid event id to event out
    * @return
    */
  def eid2EOut(eid: Int): List[Event] = eid2NOut(eid).flatMap(nodeGet(_).evq)

  /**
    * event id to node out
    * @param eid event id
    * @return
    */
  def eid2NOut(eid: Int): List[Int] = eid2Node(eid).out.toList

  /**
    * @param eid event id to event id in
    * @return
    */
  def eid2EidIn(eid: Int): List[Int] = eid2EIn(eid).map(_.eid)

  /**
    * @param eid event id to event in
    * @return
    */
  def eid2EIn(eid: Int): List[Event] = eid2NIn(eid).flatMap(nodeGet(_).evq)

  /**
    * event id to node in
    * @param eid event id
    * @return
    */
  def eid2NIn(eid: Int): List[Int] = eid2Node(eid).in.toList

  /**
    * get node by event id
    * @param eid event id
    * @return
    */
  def eid2Node(eid: Int): Node = nodeGet(emap(eid).nid)

  /**
    * get pattern by pattern id
    * @param nid pattern id
    * @return
    */
  def nodeGet(nid: Int): Node = nlacus(nid)

  /**
    * remove one event by id
    * @param eid event id
    * @return
    */
  def eventRm(eid: Int): Option[Event] = emap.synchronized(emap.remove(eid))

  /**
    * @param nid node id
    * @return
    */
  def nid2NidOut(nid: Int): mutable.Set[Int] = nlacus(nid).out

  /**
    * @param nid node id
    * @return
    */
  def nid2NidIn(nid: Int): mutable.Set[Int] = nlacus(nid).in

  /**
    * node id to event size
    * @param nid node id
    * @return
    */
  def nid2ESize(nid: Int) = nlacus(nid).evq.size

  /**
    * event id to node id
    * @param eid event id
    * @return
    */
  def eid2Nid(eid: Int) = eventGet(eid).nid

  /**
    * get event by event id
    * @param eid event id
    * @return
    */
  def eventGet(eid: Int): Event = emap(eid)

  def link(nfrom: Int, nto: Int) =
    nlacus.synchronized(nodeGet(nfrom).out.add(nto) && nodeGet(nto).in.add(nfrom))

  //  def acCacheGet(l: String): Option[(Option[Int], Boolean)] = acMap.get(l)
  //
  //  def acCacheSet(l: String, r: (Option[Int], Boolean)) = acMap.put(l, r)
  //
  //  def acCacheClear() = acMap.clear()

}
