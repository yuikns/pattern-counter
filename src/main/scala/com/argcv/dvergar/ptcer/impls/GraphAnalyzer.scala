package com.argcv.dvergar.ptcer.impls

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.argcv.dvergar.ptcer.models._
import com.argcv.valhalla.utils.Awakable

import scala.collection.mutable.{Set => MSet}
import scala.collection.parallel.immutable.{ParSeq, ParSet}
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
    val s = Source.fromFile(new File(path)).getLines()
    val nsize =  s.next().split("\t").head.toInt
    logger.info(s"graph node size: $nsize")
    val g = new Graph(nsize)
    var count = 0
    s.foreach { l =>
      val eg = l.split("\t")
      if (eg.length >= 2) {
        count += 1
        if(count % 1000000 == 0) {
          logger.info(s"graph loaded : $count")
        }
        g.link(eg(0).toInt, eg(1).toInt)
      }
    }
    g
  }

  def eventGoThrough(path: String, g: Graph, pc: PatternCounter, st: TrieTree[Int], delta: Long = 3, step: Int = 1000): Unit = {
    val lcnt = new AtomicInteger
    val timeStart = System.currentTimeMillis()
    var cts: Long = 0
    val gcDelta = delta * (MAX_NODE_SIZE - 1)
    val gcTimeout = delta
    Source.fromFile(new File(path)).getLines().foreach { l =>
      val info = l.split("\t")
      if (info.length == 2) {
        if (lcnt.incrementAndGet() % step == 0) {
          logger.info(s"event[${lcnt.get()}] time cost : ${System.currentTimeMillis() - timeStart} ms")
        }
        val nid = info(0).toInt
        val ts = info(1).toLong

        // gc section
        if (ts - cts > gcTimeout) {
          g.eventGC(ts, gcDelta)
          cts = ts
        }

        // event add
        val eid = g.eventAdd(nid, ts)
        //logger.info(s"eid: $eid")
        val detectedNids: List[Int] = List[Int](g.eid2Nid(eid))
        val nids2Ignore: MSet[List[Int]] = MSet[List[Int]]()
        eventCheck(
          ae = eid,
          an = g.eid2Nid(eid),
          delta = delta,
          detectedNids = detectedNids,
          nids2Ignore = nids2Ignore,
          g = g,
          pc = pc,
          st = st)
      }
    }
  }

  //
  //  /**
  //    * get all situation count
  //    * @param cands List[(AllCount,RquireCount)]
  //    * @return
  //    */
  //  def combSize(cands: List[(Int, Int)]): Int = {
  //    def comb(all: Int, req: Int): Int =
  //      (1 to req).par.foldLeft(((all - req + 1) to all).product)(_ / _)
  //    cands.foldLeft(1) { (l, c) =>
  //      if (c._1 < c._2) {
  //        0
  //      } else {
  //        l * comb(c._1, c._2)
  //      }
  //    }
  //  }
  //
  //  /**
  //    * return posiblity of combine
  //    * @param n2c node id to check
  //    * @param an active node
  //    * @param g graph
  //    * @return
  //    */
  //  def combCheck(n2c: List[Int], an: Int, g: Graph) = {
  //    val n2cs: (List[Int], List[Int]) = n2c.splitAt(1)
  //    val cclist = n2cs._2.groupBy(e => e)
  //      .toList
  //      .map { nd =>
  //        val nid = nd._1
  //        val rqe = nd._2.length
  //        val cap = if (nd._1 == an) g.nid2ESize(nid) - 1 else g.nid2ESize(nid)
  //        (cap, rqe)
  //      }
  //    //println(s"cclst: $cclist")
  //    combSize(cclist)
  //  }

  /**
    * return posiblity of combine
    * @param n2c node id to check
    * @param g graph
    * @return
    * @param delta delta
    */
  def combCheck(n2c: List[Int], g: Graph, delta: Long) = {
    /**
      * node id left is greater than node right?
      * one assumption is that nidl and nidr are neighbors
      * @param nidl node id left
      * @param nidr node id right
      * @return
      */
    def tsChk(nidl: Int, nidr: Int) = {
      if (g.nlacus(nidl).in.contains(nidr)) {
        Some(Tuple2(Set[Int](nidl, nidr), nidl)) // nidl > nidr
      } else if (g.nlacus(nidl).out.contains(nidr)) {
        Some(Tuple2(Set[Int](nidl, nidr), nidr)) // nidr > nidl
      } else {
        None
      }
    }

    /*
     * two nids, value shall greater
     */
    val nmap: Map[Set[Int], Int] = (for {
      i <- 0 until (n2c.length - 1)
      j <- (i + 1) until n2c.length
    } yield tsChk(n2c(i), n2c(j))).flatten.toMap

    //val eid2ts: ParSeq[(Int, Long)] = n2c.par.flatMap(g.nodeGet(_).evq.map(e=>(e.eid,e.ts)))
    val nlist: List[Node] = n2c.map(g.nodeGet)
    val an = nlist.head.nid
    val ae = nlist.head.evq.head.eid
    val ts = nlist.head.evq.head.ts

    val eid2Info: Map[Int, (Int, Long)] = nlist.flatMap(n => n.evq.map(e => (e.eid, (e.nid, e.ts)))).toMap

    val evql = for (i <- 1 until nlist.length) yield {
      val pn = nlist(i - 1)
      val n = nlist(i)
      Tuple2(n.nid, n.evq.map(e => (e.eid, e.ts)))
    }

    val stopOffset = n2c.length - 1
    def combDiscov(ae: Int = ae, ts: Long = ts, cset: Set[Int] = Set[Int](ae), offset: Int = 0): ParSet[Set[Int]] = {
      if (offset < stopOffset) {
        val evq = evql(offset)
        evq._2.par.flatMap { ev =>
          if (!cset.contains(ev._1)) {
            val nset = cset + ev._1
            val tnlist: List[Int] = nset.toList
            if ((for {
              i <- 0 until (tnlist.length - 1)
              j <- (i + 1) until tnlist.length
            } yield (tnlist(i), tnlist(j))).
              forall(evel => {
                val el = evel._1
                val er = evel._2
                val infol = eid2Info(el)
                val infor = eid2Info(er)
                nmap.get(Set[Int](infol._1, infor._1)) match {
                  case Some(gtn) =>
                    if (infol._1 == gtn) {
                      infol._2 > infor._2 && infol._2 - infor._2 <= delta
                    } else {
                      infor._2 > infol._2 && infor._2 - infor._2 <= delta
                    }
                  case None =>
                    true
                }
              })) {
              combDiscov(ev._1, ev._2, nset, offset + 1)
            } else {
              Set[Set[Int]]()
            }
          } else {
            Set[Set[Int]]()
          }
        }.toSet
      } else {
        ParSet(cset)
      }
    }
    val igg: ParSet[Set[Int]] = combDiscov()
    if (igg.isEmpty) {
      (0, None)
    } else {
      (igg.size, Some(ae +: igg.head.toList.filterNot(_ == ae)))
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
                 delta: Long,
                 detectedNids: List[Int],
                 nids2Ignore: MSet[List[Int]],
                 g: Graph,
                 pc: PatternCounter,
                 st: TrieTree[Int]): Unit = {
    val neighborNids = detectedNids.par.flatMap { nid =>
      g.nid2NidOut(nid).toList ::: g.nid2NidIn(nid).toList
    }.distinct.filter(g.nid2ESize(_) > 0)

    val pendingList: ParSeq[List[Int]] = neighborNids.flatMap { (nid: Int) =>
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
      //println(s"### nids: $nids2Check")
      val csz = combCheck(nids2Check, g, delta)
      //println(s" ## $csz")
      if (csz._1 > 0) {
        val eids = csz._2.get
        //println(s"eids: ${eids.mkString(",")} , nids: ${nids2Check.mkString(",")}")
        new Discoverer(g, eids).
          discoverOne(eids.head) match {
          case Some(path: List[Edge]) =>
            val rst: (Option[Int], Boolean) = st.check(path)
            if (rst._1.isDefined) {
              pc.add(rst._1.get, csz._1.toLong)
            }
            if (rst._2 && nids2Check.size < MAX_NODE_SIZE) {
              eventCheck(ae, an, delta, nids2Check, nids2Ignore, g, pc, st)
            }
          case None =>
        }
      }
    }
  }

}
