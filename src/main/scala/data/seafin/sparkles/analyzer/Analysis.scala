package data.seafin.sparkles.analyzer

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.spark.AggregateValue
import data.seafin.sparkles.util.Common._

/**
  * Analysis result and recommendation from running [[Analyzer]].
  */
case class Analysis(name: String) {
  private val result = new StringBuilder()
  val recommend = Recommendation()

  def addln(x: Any): Unit = {
    result.append(x).append("\n")
  }

  def add(x: Any): Unit = {
    result.append(x)
  }

  def addTopStageSizes[T <: Long](
      header: String,
      values: List[(StageId, Int, Option[AggregateValue[T]])]
  ): Unit = {
    addln(header)
    addln(s"Stage-ID    Task    ----------Size-----------")
    addln(s"           Count     Total | Average |   Max")
    values.foreach { case (stageId, numTasks, agg) =>
      agg match {
        case Some(agg) =>
          addln(
            f"${stageId}%8s ${numTasks}%7s  ${byteToString(agg.sum)}%8s  ${byteToString(agg.mean.toLong)}%8s  ${byteToString(agg.max)}%6s"
          )
        case None =>
      }
    }
    addln("")
  }

  def addTopStageDurations[T <: Long](
      header: String,
      values: List[(StageId, Int, Option[AggregateValue[T]])]
  ): Unit = {
    addln(header)
    addln(s"Stage-ID    Task    --------Duration---------")
    addln(s"           Count     Total  | Average |   Max")
    values.foreach { case (stageId, numTasks, agg) =>
      agg match {
        case Some(agg) =>
          addln(
            f"${stageId}%8s ${numTasks}%7s  ${printDuration(agg.sum)}%9s ${printDuration(agg.mean.toLong)}%9s ${printDuration(agg.max)}%9s "
          )
        case None =>
      }

    }
    addln("")
  }

  def show = {
    println(s"$name\n")
    println(result)
  }

  def toJson(): JValue = {
    ("Name" -> name) ~
      ("Result" -> result.toString)
  }
}
