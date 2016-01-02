package com.argcv.dvergar.ptcer.models

/**
 * @param eid event id (auto increment id)
 * @param nid pattern id
 * @param ts timestamp
 * @param ein pattern in
 * @param eout node out
 */
case class Event(eid: Int, nid: Int, ts: Int, eout: Option[List[Int]] = None, ein: Option[List[Int]] = None) {
  lazy val tMonitor = new AnyRef
  var tFlag = -1
  var tSeq = 0

  def getOrSetSeq(ntFlag: Int, ntSeq: Int) = tMonitor.synchronized {
    if (ntFlag == tFlag) tSeq
    else {
      tSeq = ntSeq
      tSeq
    }
  }

  def withOut(eout: List[Int]) = this.copy(eout = Some(eout))

  def withIn(ein: List[Int]) = this.copy(ein = Some(ein))

  override def toString = {
    if (ein.isDefined && eout.isDefined)
      s"[Event#1] event($eid), node($nid), t($ts), " +
        s"_in: ${ein.get.mkString(",")}, " +
        s"_out: ${eout.get.mkString(",")}"
    else
      s"[Event#0] event($eid), node($nid), t($ts)"
  }

}
