package com.argcv.dvergar.ptcer.impls

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.argcv.dvergar.ptcer.models._
import com.argcv.valhalla.utils.Awakable

import scala.collection.mutable.{ Set => MSet }
import scala.io.Source

/**
 * @author yu
 */
object GraphAnalyzer extends Awakable {
  var MAX_NODE_SIZE = 0

  def loadPatterns(path: String = "data/pattern"): List[Graph] = {
    val s: Iterator[String] = Source.fromFile(new File(path)).getLines()
    s.map { l =>
      val pg = l.split(",")
      val nsize: Int = pg(0).toInt
      val pl = pg(1).split(" ").map(_.toInt).grouped(2).toArray
      val g = new Graph(nsize)
      pl.foreach { e =>
        g.link(e(0), e(1))
      }
      (0 until nsize).foreach(g.eventAdd(_, 0))
      g
    }.toList
  }

  /**
   * @param gl graph list
   * @return
   */
  def buildSearchTree(gl: List[Graph]) = {
    val st = TrieTree[Int]()
    gl.zipWithIndex.foreach { g =>
      new Discoverer(g._1, g._1.emap.keys.toList).
        discoverAll().
        foreach(st.put(_, g._2))
    }
    st
  }

  /**
   * load all graph nodes
   * @param path e.g. "data/dataset/ER_small/graph.txt"
   * @return
   */
  def loadGraph(path: String): Graph = {
    val s = Source.fromFile(new File(path)).getLines().toArray.splitAt(1)
    val nsize = s._1.head.split("\t").head.toInt
    val g = new Graph(nsize)
    s._2.foreach { l =>
      val eg = l.split("\t")
      if (eg.length >= 2) {
        g.link(eg(0).toInt, eg(1).toInt)
      }
    }
    g
  }

  def eventGoThrough(path: String, g: Graph, pc: PatternCounter, st: TrieTree[Int], delta: Int = 3, step: Int = 1000): Unit = {
    val lcnt = new AtomicInteger
    val timeStart = System.currentTimeMillis()
    var ctime = -1
    Source.fromFile(new File(path)).getLines().foreach { l =>
      val info = l.split("\t")
      if (info.length == 2) {
        if (lcnt.incrementAndGet() % step == 0) {
          logger.info(s"event[${lcnt.get()}] time cost : ${System.currentTimeMillis() - timeStart} ms")
        }
        val nid = info(0).toInt
        val ts = info(1).toInt
        if (ts != ctime) {
          ctime = ts
          g.eventGC(ts, delta)
        }
        val eid = g.eventAdd(nid, ts)
        //logger.info(s"eid: $eid")
        val pendingSet = List[Int](g.eid2Nid(eid))
        val checkedSet = MSet[List[Int]]()
        eventCheck(
          ae = eid,
          an = g.eid2Nid(eid),
          detectedNids = pendingSet,
          nids2Ignore = checkedSet,
          g = g,
          pc = pc,
          st = st)
      }
    }
  }

  /**
   * get all situation count
   * @param cands List[(AllCount,RquireCount)]
   * @return
   */
  def combSize(cands: List[(Int, Int)]) = {
    def comb(all: Int, req: Int): Int =
      (1 to req).par.foldLeft(((all - req + 1) to all).product)(_ / _)
    cands.foldLeft(1) { (l, c) =>
      if (c._1 < c._2) {
        0
      } else {
        l * comb(c._1, c._2)
      }
    }
  }

  /**
   * @param ae active event id
   * @param detectedNids pending set
   * @param nids2Ignore checked set
   * @param g graph
   * @param pc pattern counter
   * @param st trie tree
   * @param an node id of active event stored in
   */
  def eventCheck(ae: Int,
    an: Int,
    detectedNids: List[Int],
    nids2Ignore: MSet[List[Int]],
    g: Graph,
    pc: PatternCounter,
    st: TrieTree[Int]): Unit = {
    val neighborNids = detectedNids.par.flatMap { nid =>
      g.nid2NidOut(nid).toList ::: g.nid2NidIn(nid).toList
    }.distinct.filter(g.nid2ESize(_) > 0)

    val pendingList = neighborNids.flatMap { (nid: Int) =>
      val tmpList: List[Int] = detectedNids.toList :+ nid
      val slist = tmpList.sortWith(_ > _)
      nids2Ignore.synchronized {
        if (nids2Ignore.contains(slist)) {
          None
        } else {
          nids2Ignore.add(slist)
          Some(tmpList)
        }
      }
    }
    pendingList.foreach { (nids2Check: List[Int]) =>
      def combCheck(n2c: List[Int]): Int = {
        val n2cs: (List[Int], List[Int]) = n2c.splitAt(1)
        val cclist = n2cs._2.groupBy(e => e)
          .toList
          .map { nd =>
            val nid = nd._1
            val rqe = nd._2.length
            val cap = if (nd._1 == an) g.nid2ESize(nid) - 1 else g.nid2ESize(nid)
            (cap, rqe)
          }
        //println(s"cclst: $cclist")
        combSize(cclist)
      }
      //println(s"### nids: $nids2Check")
      val csz = combCheck(nids2Check)
      //println(s" ## $csz")
      if (csz > 0) {
        val eidSet = MSet[Int]()
        //println(s"nids: $nids2Check , csz : $csz")

        val eids: List[Int] = nids2Check.map { nid =>
          val epool = g.nodeGet(nid).evq
          val ev: Event = epool.find(e => !eidSet.contains(e.eid)).get
          eidSet.add(ev.eid)
          ev.eid
        }
        new Discoverer(g, eids).
          discoverOne(eids.head) match {
            case Some(path: List[Edge]) =>
              val rst: (Option[Int], Boolean) = st.check(path)
              if (rst._1.isDefined) {
                pc.add(rst._1.get, csz)
              }
              if (rst._2 && nids2Check.size < MAX_NODE_SIZE) {
                eventCheck(ae, an, nids2Check, nids2Ignore, g, pc, st)
              }
            case None =>
          }

        //        val slist = nids2Check.sortWith(_ > _).mkString("-")
        //
        //        val rt: Option[(Option[Int], Boolean)] = g.acCacheGet(slist) match {
        //          case Some(rt: (Option[Int], Boolean)) =>
        //            Some(rt)
        //          case None =>
        //            val eids: List[Int] = nids2Check.map { nid =>
        //              val epool = g.nodeGet(nid).evq
        //              val ev: Event = epool.find(e => !eidSet.contains(e.eid)).get
        //              eidSet.add(ev.eid)
        //              ev.eid
        //            }
        //            new Discoverer(g, eids).
        //              discoverOne(eids.head) match {
        //                case Some(path: List[Edge]) =>
        //                  val rst: (Option[Int], Boolean) = st.check(path)
        //                  g.acCacheSet(slist, rst)
        //                  Some(rst)
        //                case None =>
        //                  None
        //              }
        //        }
        //        if (rt.isDefined) {
        //          val rst = rt.get
        //          if (rst._1.isDefined) {
        //            pc.add(rst._1.get, csz)
        //          }
        //          if (rst._2 && nids2Check.size < MAX_NODE_SIZE) {
        //            eventCheck(ae, an, nids2Check, nids2Ignore, g, pc, st)
        //          }
        //        }

      }
    }
  }

}
