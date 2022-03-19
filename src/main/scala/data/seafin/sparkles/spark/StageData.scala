package data.seafin.sparkles.spark

import scala.collection.mutable

import net.liftweb.json.JsonDSL._
import net.liftweb.json._
import org.apache.spark.storage.RDDInfo

import data.seafin.sparkles.util.Common._

/**
  * Stage level metrics.
  */
case class StageData(
    stageId: StageId,
    jobId: JobId,
    name: String,
    numTasks: Int,
    startTime: TimeMs,
    endTime: TimeMs,
    duration: TimeMs,
    taskAgg: Option[TaskDataAggregation],
    parentStageIDs: Seq[StageId],
    rddInfos: Seq[RDDInfo],
    failureReason: Option[String] = None
) {
  def toJson(): JValue = {
    ("StageID" -> stageId) ~
      ("JobID" -> jobId) ~
      ("StageName" -> name) ~
      ("NumTasks" -> numTasks) ~
      ("StartTime" -> startTime) ~
      ("EndTime" -> endTime) ~
      ("Duration" -> duration) ~
      ("ParentStageIDs" -> parentStageIDs) ~
      ("FailureReason" -> failureReason) ~
      // TODOS: Add RDDInfos
      ("TaskAggregation" -> taskAgg.map(_.toJson))
  }
}

object StageData {
  def apply(partialData: PartialStageData): StageData = {
    new StageData(
      partialData.stageId,
      partialData.jobId,
      partialData.name,
      partialData.numTasks,
      partialData.startTime,
      partialData.endTime,
      partialData.endTime - partialData.startTime,
      TaskDataAggregation(partialData.tasks.toList),
      partialData.parentStageIDs,
      partialData.rddInfos,
      partialData.failureReason
    )
  }
}

/**
  * Partial stage data while collecting.
  */
case class PartialStageData(
    stageId: StageId,
    jobId: JobId,
    name: String,
    numTasks: Int,
    startTime: TimeMs,
    parentStageIDs: Seq[StageId],
    var rddInfos: Seq[RDDInfo]
) {
  var endTime: TimeMs = _
  var failureReason: Option[String] = None
  val tasks = new mutable.ListBuffer[TaskData]()
}
