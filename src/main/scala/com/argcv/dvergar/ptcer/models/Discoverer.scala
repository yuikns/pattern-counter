package com.argcv.dvergar.ptcer.models

import scala.collection.mutable.{ Map => MMap }
import scala.collection.parallel.immutable.{ ParMap, ParSeq }

/**
 * @author yu
 * @param g graph
 * @param eids event ids to be checked
 */
class Discoverer(g: Graph, eids: List[Int]) {

  /**
   * allocate all event id with links
   * this is just a subgraph with list ids
   */
  lazy val events: ParMap[Int, Event] = {
    // this section will add event with nout and nin
    // event id , event , nid, node
    val evPackSeq: ParSeq[(Int, Event, Int, Node)] =
      eids.par.map { eid =>
        val ev = g.eventGet(eid)
        val nd = g.nodeGet(ev.nid)
        (eid, ev, ev.nid, nd)
      }
    val nid2Nodes: ParMap[Int, ParSeq[(Int, Event, Int, Node)]] =
      evPackSeq.groupBy(e => e._3)
    val nid2NodeIOs: ParMap[Int, (List[Int], List[Int])] =
      nid2Nodes.map { (el: (Int, ParSeq[(Int, Event, Int, Node)])) =>
        val cnd: Node = el._2.head._4 // current node
        el._1 -> Tuple2(
          cnd.in.filter(nid2Nodes.contains).toList,
          cnd.out.filter(nid2Nodes.contains).toList)
      }
    val nid2Eids: ParMap[Int, List[Int]] = nid2Nodes.map(e => e._1 -> e._2.map(_._1).toList)
    evPackSeq.map { evPack =>
      val nodeIOs = nid2NodeIOs(evPack._3)
      evPack._1 ->
        evPack._2.copy(
          ein = Some(nodeIOs._1.
            flatMap(pid => nid2Eids(pid)).
            filterNot(_ == evPack._1).
            distinct),
          eout = Some(nodeIOs._2.
            flatMap(pid => nid2Eids(pid)).
            filterNot(_ == evPack._1).
            distinct))
    }.toMap // eid -> event with io
  }

  /**
   * one path from current active event
   * @return
   * @param label current label
   * @param e2l event id 2 label
   * @param ae active event id
   * @param echain edge chain
   */
  def discoverSingle(echain: List[Edge], ae: Int, e2l: Map[Int, Int], label: Int, eSet: Set[Edge]): List[(List[Edge], Map[Int, Int], Int, Set[Edge])] = {
    /**
     * current event
     */
    val ev: Event = events(ae)

    /**
     * (next event id , reverse)
     * out : false
     * in : true
     */
    val eios: List[(Int, Boolean)] = ev.eout.get.map(_ -> false) ::: ev.ein.get.map(_ -> true)

    /**
     * active event label
     */
    val ael = e2l(ae)

    eios.foldLeft(List[(List[Edge], Map[Int, Int], Int, Set[Edge])](Tuple4(echain, e2l, label, eSet))) {
      (l, c) =>
        l.take(1).flatMap { e =>
          val ceid: Int = c._1
          val cio: Boolean = c._2
          val cechain: List[Edge] = e._1
          val ce2l: Map[Int, Int] = e._2
          val clabel: Int = e._3
          val cESet = e._4
          // current active id
          ce2l.get(ceid) match {
            case Some(ulabel) =>
              val newEdge = Edge(ael, ulabel, cio)
              if (cESet.contains(newEdge.norm())) List(e)
              else List(Tuple4(e._1 :+ newEdge, e._2, e._3, e._4 + newEdge.norm()))
            //              if (cESet.contains(newEdge)) List(e)
            //              else List(Tuple4(e._1 :+ newEdge, e._2, e._3, e._4 + newEdge))
            case None =>
              val newEdge = Edge(ael, clabel, cio)
              discoverSingle(
                cechain :+ newEdge,
                ceid, ce2l + (ceid -> clabel),
                clabel + 1,
                cESet + newEdge.norm())
            //              discoverSingle(
            //                cechain :+ newEdge,
            //                ceid, ce2l + (ceid -> clabel),
            //                clabel + 1,
            //                cESet + newEdge)
          }
        }.take(1)
    }
  }

  def discoverOne(ae: Int): Option[List[Edge]] = {
    discoverSingle(List[Edge](), ae, Map[Int, Int](ae -> 0), 1, Set[Edge]()).
      par.
      map(_._1).
      //map(_.map(_.norm()).sortWith((l, r) => if (l.i == r.i) l.o > r.o else l.i > r.i)).
      headOption
  }

  /**
   * all path from different id
   * @return
   * @param label current label
   * @param e2l event id 2 label
   * @param ae active event id
   * @param echain edge chain
   */
  def discoverMulti(echain: List[Edge], ae: Int, e2l: Map[Int, Int], label: Int, eSet: Set[Edge]): List[(List[Edge], Map[Int, Int], Int, Set[Edge])] = {
    /**
     * current event
     */
    val ev: Event = events(ae)

    /**
     * (next event id , reverse)
     * out : false
     * in : true
     */
    val eioCands: List[(Int, Boolean)] = ev.eout.get.map(_ -> false) ::: ev.ein.get.map(_ -> true)

    /**
     * active event label
     */
    val ael = e2l(ae)

    def fullArray[T](cands: List[T]): List[List[T]] = {
      cands.length match {
        case 0 =>
          List[List[T]]()
        case 1 =>
          List[List[T]](List[T](cands.head))
        case _ =>
          cands.flatMap { c =>
            fullArray(cands.filter(_ != c)).map(_ :+ c)
          }
      }
    }

    fullArray(eioCands).flatMap { eios =>
      eios.foldLeft(List[(List[Edge], Map[Int, Int], Int, Set[Edge])](Tuple4(echain, e2l, label, eSet))) {
        (l: List[(List[Edge], Map[Int, Int], Int, Set[Edge])], c) =>
          l.flatMap { e =>
            val ceid: Int = c._1
            val cio: Boolean = c._2
            val cechain: List[Edge] = e._1
            val ce2l: Map[Int, Int] = e._2
            val clabel: Int = e._3
            val cESet = e._4
            // current active id
            ce2l.get(ceid) match {
              case Some(ulabel) =>
                val newEdge = Edge(ael, ulabel, cio)
                if (cESet.contains(newEdge.norm())) List(e)
                else List(Tuple4(e._1 :+ newEdge, e._2, e._3, e._4 + newEdge.norm()))
              //                if (cESet.contains(newEdge)) List(e)
              //                else List(Tuple4(e._1 :+ newEdge, e._2, e._3, e._4 + newEdge))
              case None =>
                val newEdge = Edge(ael, clabel, cio)
                discoverMulti(
                  cechain :+ newEdge,
                  ceid, ce2l + (ceid -> clabel), clabel + 1, cESet + newEdge.norm())
              //                discoverMulti(
              //                  cechain :+ newEdge,
              //                  ceid, ce2l + (ceid -> clabel), clabel + 1, cESet + newEdge)
            }
          }.distinct
      }
    }
  }

  def discoverAll() = {
    eids.par.
      flatMap(ae =>
        discoverMulti(List[Edge](), ae, Map[Int, Int](ae -> 0), 1, Set[Edge]()).par.map(_._1)).
      distinct.
      //map(_.map(_.norm()).sortWith((l, r) => if (l.i == r.i) l.o > r.o else l.i > r.i)).
      toList
  }

}
