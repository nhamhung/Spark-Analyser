package data.seafin.sparkles.spark

import scala.collection.mutable

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.util.Common._

/**
  * Executor level metrics.
  */
case class ExecutorData(
    executorId: String,
    cores: Int,
    startTime: TimeMs,
    endTime: TimeMs,
    duration: TimeMs,
    taskAgg: Option[TaskDataAggregation]
) {
  def toJson(): JValue = {
    ("ExecutorID" -> executorId) ~
      ("Cores" -> cores) ~
      ("StartTime" -> startTime) ~
      ("EndTime" -> endTime) ~
      ("Duration" -> duration) ~
      ("TaskAggregation" -> taskAgg.map(_.toJson))
  }
}

object ExecutorData {
  def apply(partialData: PartialExecutorData): ExecutorData = {
    new ExecutorData(
      partialData.executorId,
      partialData.cores,
      partialData.startTime,
      partialData.endTime,
      partialData.endTime - partialData.startTime,
      TaskDataAggregation(partialData.tasks.toList)
    )
  }
}

/**
  * Partial executor data while collecting.
  */
case class PartialExecutorData(executorId: String, cores: Int, startTime: TimeMs) {
  var endTime: TimeMs = _
  val tasks = new mutable.ListBuffer[TaskData]()
}
