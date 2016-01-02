package com.argcv.dvergar.ptcer.models

/**
 * @author yu
 * @param r reverse
 * @param i input
 * @param o output
 */
case class Edge(i: Int, o: Int, r: Boolean) {
  def toString(m: Map[Int, Int]): String = {
    try {
      Edge(m(i), m(o), r).toString
    } catch {
      case t: Throwable =>
        "(>>" + t.getMessage + " map:" + m + "<<)"
    }
  }

  override def toString = r match {
    case false => s"($i->$o)"
    case true => s"($i<-$o)"
  }

  def norm() = r match {
    case false => this
    case true => Edge(i = o, o = i, r = false)
  }
}

//case class Edge(i: Int)

