package data.seafin.sparkles.spark

import scala.collection.mutable

import org.apache.spark.storage.RDDInfo

import data.seafin.sparkles.util.Common._

/**
  * Build [[spark.AppData]] from Spark event stream.
  */
class AppDataBuilder {
  import AppDataBuilder._

  var appId: String = _
  var appName: String = _
  var appStartTime: TimeMs = 0L
  var appEndTime: TimeMs = 0L
  var queueInfo: QueueInfo = _
  var appConfig: AppConfig = _
  val executorMap = new mutable.HashMap[String, PartialExecutorData]
  val jobMap = new mutable.HashMap[JobId, PartialJobData]
  val stageMap = new mutable.HashMap[StageId, PartialStageData]
  val taskMap = new mutable.HashMap[TaskId, TaskData]

  def addAppInfo(id: String, name: String): Unit = {
    appId = id
    appName = name
  }

  def addAppStartTime(time: TimeMs): Unit = {
    appStartTime = time
  }

  def addAppEndTime(time: TimeMs): Unit = {
    appEndTime = time
  }

  def addQueueInfo(info: QueueInfo): Unit = {
    queueInfo = info
  }

  def addAppConfig(config: AppConfig): Unit = {
    appConfig = config
  }

  def addExecutorData(data: PartialExecutorData): Unit = {
    executorMap.getOrElseUpdate(data.executorId, data)
  }

  def addExecutorEndTime(executorId: String, endTime: TimeMs): Unit = {
    executorMap(executorId).endTime = endTime
  }

  def addJobData(data: PartialJobData): Unit = {
    jobMap.getOrElseUpdate(data.jobId, data)
  }

  def addJobEndTime(jobId: JobId, endTime: TimeMs): Unit = {
    jobMap(jobId).endTime = endTime
  }

  def addStageData(data: PartialStageData): Unit = {
    stageMap.getOrElseUpdate(data.stageId, data)
    jobMap(data.jobId).stageMap(data.stageId) = data
  }

  def addStageEndTime(stageId: StageId, endTime: TimeMs): Unit = {
    stageMap(stageId).endTime = endTime
  }

  def addStageRddInfo(stageId: StageId, rddInfos: Seq[RDDInfo]): Unit = {
    stageMap(stageId).rddInfos = rddInfos
  }

  def addStageFailureReason(stageId: StageId, failureReason: Option[String]): Unit = {
    stageMap(stageId).failureReason = failureReason
  }

  def addTaskData(data: TaskData): Unit = {
    taskMap.getOrElseUpdate(data.taskId, data)
    executorMap(data.executorId).tasks += data
    jobMap(data.jobId).tasks += data
    stageMap(data.stageId).tasks += data
  }

  def build(): AppData = {
    // set end times for the jobs for which onJobEnd was missed
    jobMap.map {
      case (jobId, jobData) => {
        // if job's endTime was missed
        if (jobData.endTime == 0) {
          // if got stages, set endTime to max stage's endTime
          if (!jobData.stageMap.isEmpty) {
            jobMap(jobId).endTime = jobData.stageMap.map { case (_, stageData) =>
              stageData.endTime
            }.max
          } else {
            // if no stages, set job's endTime to app's endTime
            jobMap(jobId).endTime = appEndTime
          }
        }
      }
    }

    // set end times for executors if not removed
    executorMap.map {
      case (executorId, executorData) => {
        if (executorData.endTime == 0) {
          if (executorData.tasks.size > 0) {
            executorMap(executorId).endTime = executorData.tasks.map(_.endTime).max
          }
        } else {
          executorMap(executorId).endTime = appEndTime
        }
      }
    }

    new AppData(
      appId,
      appName,
      appStartTime,
      appEndTime,
      appEndTime - appStartTime,
      queueInfo,
      appConfig,
      executorMap.map { case (_, partialData) => ExecutorData(partialData) }.toList.sorted,
      jobMap.map { case (_, partialData) => JobData(partialData) }.toList.sorted,
      stageMap.map { case (_, partialData) => StageData(partialData) }.toList.sorted,
      taskMap.values.toList.sorted
    )
  }
}

object AppDataBuilder {
  // ordering data by Id
  implicit val executorIdOrdering: Ordering[ExecutorData] = Ordering.by(_.executorId)
  implicit val jobIdOrdering: Ordering[JobData] = Ordering.by(_.jobId)
  implicit val stageIdOrdering: Ordering[StageData] = Ordering.by(_.stageId)
  implicit val taskIdOrdering: Ordering[TaskData] = Ordering.by(_.taskId)
}
