package data.seafin.sparkles.spark

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.util.Common._

/**
  * Stores Spark app data collected from Spark event stream.
  */
case class AppData(
    appId: String,
    appName: String,
    startTime: TimeMs,
    endTime: TimeMs,
    duration: TimeMs,
    queueInfo: QueueInfo,
    appConfig: AppConfig,
    executors: List[ExecutorData],
    jobs: List[JobData],
    stages: List[StageData],
    tasks: List[TaskData]
) {
  def toJson(): JValue = {
    ("AppID" -> appId) ~
      ("AppName" -> appName) ~
      ("StartTime" -> startTime) ~
      ("EndTime" -> endTime) ~
      ("Duration" -> duration) ~
      ("QueueInfo" -> queueInfo.toJson) ~
      ("AppConfig" -> appConfig.toJson) ~
      ("Executors" -> JArray(executors.map(_.toJson))) ~
      ("Jobs" -> JArray(jobs.map(_.toJson))) ~
      ("Stages" -> JArray(stages.map(_.toJson))) ~
      ("Tasks" -> JArray(tasks.map(_.toJson)))
  }
}
