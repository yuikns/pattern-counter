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
        logger.info(s"eid: $eid")
        val pendingSet = Set[Int](eid)
        val checkedSet = MSet[Set[Int]]()
        val ig = new AtomicInteger()
        eventCheck(
          ae = eid,
          rBount = g.eventGet(eid).getOrSetSeq(eid, ig.getAndIncrement()),
          ig = ig,
          pendingSet = pendingSet,
          checkedSet = checkedSet,
          g = g,
          pc = pc,
          st = st)
      }
    }
  }

  def eventCheck(ae: Int,
    rBount: Int,
    ig: AtomicInteger,
    pendingSet: Set[Int],
    checkedSet: MSet[Set[Int]],
    g: Graph,
    pc: PatternCounter,
    st: TrieTree[Int]): Unit = {
    val neighbors = pendingSet.flatMap { eid =>
      g.eid2EIn(eid) ::: g.eid2EOut(eid)
    }.filter(_.getOrSetSeq(ae, ig.getAndIncrement()) > rBount).map(_.eid)
    //    val aliveSet: ParSet[Set[Int]] = neighbors.par.flatMap { eid =>
    //      val tmpSet: Set[Int] = pendingSet + eid
    //      checkedSet.synchronized {
    //        if (checkedSet.contains(tmpSet)) {
    //          None
    //        } else {
    //          checkedSet.add(tmpSet)
    //          Some(tmpSet)
    //        }
    //      }
    //    }
    neighbors.foreach { neighbor =>
      val ac = pendingSet + neighbor
      new Discoverer(g, ac.toList).
        discoverOne(ae) match {
          case Some(path) =>
            val rst = st.check(path)
            if (rst._1.isDefined) {
              pc.add(rst._1.get)
            }
            if (rst._2 && ac.size <= MAX_NODE_SIZE) {
              eventCheck(ae, g.eventGet(neighbor).tSeq, ig, ac, checkedSet, g, pc, st)
            }
          case None =>
        }
    }
  }

  //
  //  def eventCheck(ae: Int,
  //                 pendingSet: Set[Int],
  //                 checkedSet: MSet[Set[Int]],
  //                 g: Graph,
  //                 pc: PatternCounter,
  //                 st: TrieTree[Int]): Unit = {
  //    val neighbors: Set[Int] = pendingSet.flatMap { eid =>
  //      g.eid2EidIn(eid) ::: g.eid2EidOut(eid)
  //    }
  //    val aliveSet: ParSet[Set[Int]] = neighbors.par.flatMap { eid =>
  //      val tmpSet: Set[Int] = pendingSet + eid
  //      checkedSet.synchronized {
  //        if (checkedSet.contains(tmpSet)) {
  //          None
  //        } else {
  //          checkedSet.add(tmpSet)
  //          Some(tmpSet)
  //        }
  //      }
  //    }
  //    aliveSet.foreach { ac =>
  //      new Discoverer(g, ac.toList).
  //        discoverOne(ae) match {
  //        case Some(path) =>
  //          val rst = st.check(path)
  //          if (rst._1.isDefined) {
  //            pc.add(rst._1.get)
  //          }
  //          if (rst._2 && ac.size <= MAX_NODE_SIZE) {
  //            eventCheck(ae, ac, checkedSet, g, pc, st)
  //          }
  //        case None =>
  //      }
  //    }
  //  }

}
