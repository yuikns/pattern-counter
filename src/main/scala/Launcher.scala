import com.argcv.dvergar.ptcer.impls.GraphAnalyzer
import com.argcv.dvergar.ptcer.models.{ Graph, PatternCounter, TrieTree }
import org.slf4j.LoggerFactory

object Settings {
  lazy val graphSmall: String = "data/dataset/ER_small/graph.txt"
  lazy val graphLarge: String = "data/dataset/ER_large/graph.txt"

  lazy val logsSmall: String = "data/dataset/ER_small/logs.txt"
  lazy val logsLarge: String = "data/dataset/ER_large/logs.txt"

  /**
   * delta, event expired
   */
  lazy val delta: Int = 3
}

/**
 * @author yu
 */
object Launcher extends App {

  lazy val logger = LoggerFactory.getLogger(Launcher.getClass)
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
  val g: Graph = GraphAnalyzer.loadGraph(Settings.graphLarge)

  val timeGraphLoad = System.currentTimeMillis()
  logger.info(s"graph loaded, size: ${g.nlacus.length}, time cost: ${timeGraphLoad - timeCounterInit} ms")

  GraphAnalyzer.eventGoThrough(Settings.logsLarge, g, pc, st, delta = 10, step = 100)

  val timeEventPass = System.currentTimeMillis()
  logger.info(s"logs passed, time cost: ${timeEventPass - timeGraphLoad} ms")

  pc.printAll()

  logger.info(s"all done , all time cost: ${System.currentTimeMillis() - timeStart} ms")
}
