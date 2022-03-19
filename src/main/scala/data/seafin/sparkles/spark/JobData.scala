package data.seafin.sparkles.spark

import scala.collection.mutable

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.util.Common._

/**
  * Job level metrics.
  */
case class JobData(
    jobId: JobId,
    sqlExecId: String,
    startTime: TimeMs,
    endTime: TimeMs,
    duration: TimeMs,
    taskAgg: Option[TaskDataAggregation]
) {
  def toJson(): JValue = {
    ("JobID" -> jobId) ~
      ("SQLExecID" -> sqlExecId) ~
      ("StartTime" -> startTime) ~
      ("EndTime" -> endTime) ~
      ("Duration" -> duration) ~
      ("TaskAggregation" -> taskAgg.map(_.toJson))
  }
}

object JobData {
  def apply(partialData: PartialJobData): JobData = {
    new JobData(
      partialData.jobId,
      partialData.sqlExecId,
      partialData.startTime,
      partialData.endTime,
      partialData.endTime - partialData.startTime,
      TaskDataAggregation(partialData.tasks.toList)
    )
  }
}

/**
  * Partial job data while collecting.
  */
case class PartialJobData(jobId: JobId, sqlExecId: String, startTime: TimeMs) {
  var endTime: TimeMs = _
  val tasks = new mutable.ListBuffer[TaskData]()
  val stageMap = new mutable.HashMap[StageId, PartialStageData]()
}
