import com.argcv.dvergar.ptcer.impls.GraphAnalyzer
import com.argcv.dvergar.ptcer.models.{ Graph, PatternCounter, TrieTree }
import org.slf4j.LoggerFactory

case class ArgsOpt(
  dir: String = "",
  delta: Int = 3,
  step: Int = 20) {

  override def toString =
    s"dir : $dir\ngraph : $graph\n" +
      s"logs : $logs\ndelta : $delta\nstep : $step"

  def logs = s"$dir/logs.txt"

  def graph = s"$dir/graph.txt"

  def nodeDict = s"$dir/node_dict.txt"

  def init() = this.suit(suitCands("small"))

  def suit(s: String) = this.copy(dir = s"data/dataset/$s")

  def suitCands = Map[String, String](
    "small" -> "ER_small",
    "middle" -> "ER_middle",
    "large" -> "ER_large"
  )

  def withArgs(args: Array[String], offset: Int): ArgsOpt = {
    //lazy val logger = LoggerFactory.getLogger(this.getClass)
    def noEnoughParameterErrorLog(s: String): Unit = {
      println(s"no enough parameter for $s")
    }
    def unExpectParameterErrorLog(s: String, e: String): Unit = {
      println(s"unexpect parameter after: $s $e")
    }
    if (args.length > offset) {
      args(offset) match {
        case "-d" => // delta
          if (args.length > offset) {
            val nxArgs: (ArgsOpt, Int) = try {
              (this.copy(delta = args(offset + 1).toInt), offset + 2)
            } catch {
              case t: Throwable =>
                unExpectParameterErrorLog(args(offset), t.getLocalizedMessage)
                (this, offset + 1)
            }
            nxArgs._1.withArgs(args, nxArgs._2)
          } else {
            noEnoughParameterErrorLog(args(offset))
            this
          }
        case "-l" => // log step
          if (args.length > offset) {
            val nxArgs: (ArgsOpt, Int) = try {
              (this.copy(step = args(offset + 1).toInt), offset + 2)
            } catch {
              case t: Throwable =>
                unExpectParameterErrorLog(args(offset), t.getLocalizedMessage)
                (this, offset + 1)
            }
            nxArgs._1.withArgs(args, nxArgs._2)
          } else {
            noEnoughParameterErrorLog(args(offset))
            this
          }
        case "-s" =>
          if (args.length > offset) {
            val nxArgs: (ArgsOpt, Int) = try {
              (this.copy(dir = args(offset + 1)), offset + 2)
            } catch {
              case t: Throwable =>
                unExpectParameterErrorLog(args(offset), t.getLocalizedMessage)
                (this, offset + 1)
            }
            nxArgs._1.withArgs(args, nxArgs._2)
          } else {
            noEnoughParameterErrorLog(args(offset))
            this
          }
        case "-v" =>
          this.copy(step = 1).withArgs(args, offset + 1)
        case "-h" =>
          println(s"pattern-counter [-s dir] [-l log step] [-d delta] [-v] [suit/dir]")
          this
        case s: String =>
          if (suitCands.contains(s)) {
            this.suit(suitCands(s)).withArgs(args, offset + 1)
          } else {
            this.suit(s).withArgs(args, offset + 1)
          }
      }
    } else {
      this
    }
  }
}

/**
 * @author yu
 */
object Launcher extends App {
  lazy val logger = LoggerFactory.getLogger(Launcher.getClass)
  println(s"args: $args")
  val opts = ArgsOpt().init().withArgs(args, 0)
  println(s"Options:\n$opts")
  logger.info("all starting ...")
  val timeStart = System.currentTimeMillis()

  /**
   * pattern graph init
   */
  val pgl = GraphAnalyzer.loadPatterns("data/pattern")

  GraphAnalyzer.MAX_NODE_SIZE = pgl.map(e => e.nlacus.length).sortWith(_ > _).head

  /**
   * search tree for patterns
   */
  val st: TrieTree[Int] = GraphAnalyzer.buildSearchTree(pgl)
  val timeLoadPatterns = System.currentTimeMillis()
  logger.info(s"patterns loaded, size: ${pgl.length}, time cost: ${timeLoadPatterns - timeStart} ms")

  /**
   * pattern counter
   */
  val pc = new PatternCounter(pgl.length)
  val timeCounterInit = System.currentTimeMillis()
  logger.info(s"pattern counter inited, time cost: ${timeCounterInit - timeLoadPatterns} ms")

  /**
   * current graph
   */
  val g: Graph = GraphAnalyzer.loadGraph(opts.graph)

  val timeGraphLoad = System.currentTimeMillis()
  logger.info(s"graph loaded, size: ${g.nlacus.length}, time cost: ${timeGraphLoad - timeCounterInit} ms")

  GraphAnalyzer.eventGoThrough(opts.logs, g, pc, st, delta = opts.delta, step = opts.step)

  val timeEventPass = System.currentTimeMillis()
  logger.info(s"logs passed, time cost: ${timeEventPass - timeGraphLoad} ms")

  pc.printAll()

  logger.info(s"all done , all time cost: ${System.currentTimeMillis() - timeStart} ms")
}
