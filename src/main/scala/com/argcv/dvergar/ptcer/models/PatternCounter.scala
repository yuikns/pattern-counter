package com.argcv.dvergar.ptcer.models

import java.util.concurrent.atomic.AtomicInteger

/**
 * @author yu
 */
class PatternCounter(psize: Int) {
  val counter = (0 until psize).map(i => new AtomicInteger()).toArray

  def add(i: Int): Int = counter(i).incrementAndGet()

  /**
   * @param i index
   * @param c count
   * @return
   */
  def add(i: Int, c: Int) = counter(i).addAndGet(c)

  def count(i: Int): Int = counter(i).get()

  def printAll(): Unit = {
    println("Pattern Summary:")
    counter.zipWithIndex.foreach { c =>
      //logger.info(s"[${c._2}] = ${c._1.get()}")
      if (c._2 % 5 == 4) {
        println(f"[${c._2}%2d] = ${c._1.get()}%-6d")
      } else {
        print(f"[${c._2}%2d] = ${c._1.get()}%-6d    ")
      }
    }
    println()
  }
}
