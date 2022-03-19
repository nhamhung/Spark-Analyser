package data.seafin.sparkles

import java.io.InputStream
import java.lang.reflect.Method

import net.liftweb.json._
import org.apache.spark.SparkConf

import data.seafin.sparkles.util.EventLogReader

/**
  * Main App to read event log file and replay listener.
  */
case class SparklesApp(sparkConf: SparkConf) {
  def run(): Unit = {
    val conf = Config(sparkConf)

    val listener = new SparklesListener(sparkConf)
    val (fs, logDir) = conf.getLogDirFileSystemAndPath()
    val (stream, logFile) = EventLogReader.getEventLogAsStream(sparkConf, fs, logDir, conf.appId)

    // gets ReplayListenerBus at runtime, and adds SparklesListener
    val busKlass: Class[_] = Class.forName("org.apache.spark.scheduler.ReplayListenerBus")
    val bus: Any = busKlass.newInstance()
    val addListenerMethod: Method = busKlass.getMethod("addListener", classOf[Object])
    addListenerMethod.invoke(bus, listener)

    // replays Spark event stream
    try {
      val replayMethod = busKlass.getMethod(
        "replay",
        classOf[InputStream],
        classOf[String],
        classOf[Boolean],
        classOf[String => Boolean]
      )
      replayMethod.invoke(bus, stream, logFile.toString, boolean2Boolean(false), getFilter _)
    } catch {
      case e: Exception => println(e)
    }
  }

  private def getFilter(eventString: String): Boolean = {
    implicit val formats = DefaultFormats
    val json = parse(eventString)
    eventFilter.contains((json \ "Event").extract[String])
  }

  private def eventFilter: Set[String] = {
    Set(
      "SparkListenerApplicationStart",
      "SparkListenerApplicationEnd",
      "SparkListenerEnvironmentUpdate",
      "SparkListenerExecutorAdded",
      "SparkListenerExecutorRemoved",
      "SparkListenerJobStart",
      "SparkListenerJobEnd",
      "SparkListenerStageSubmitted",
      "SparkListenerStageCompleted",
      "SparkListenerTaskEnd"
    )
  }
}

object SparklesApp extends App {
  val sparkConf = new SparkConf()
  val app = new SparklesApp(sparkConf)
  app.run()
}
