package data.seafin.sparkles.spark

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.util.Common._

/**
  * Task metrics.
  */
case class TaskData(
    taskId: TaskId,
    jobId: JobId,
    stageId: StageId,
    executorId: String,
    startTime: TimeMs,
    endTime: TimeMs,
    duration: TimeMs,
    executorRunTime: TimeMs,
    executorCpuTime: TimeMs,
    gettingResultTime: TimeMs,
    schedulerDelay: TimeMs,
    jvmGcTime: TimeMs,
    peakExecutionMemory: Bytes,
    resultSize: Bytes,
    memoryBytesSpilled: Bytes,
    diskBytesSpilled: Bytes,
    shuffleWriteTime: TimeMs,
    shuffleBytesWritten: Bytes,
    shuffleRecordsWritten: Long,
    shuffleFetchWaitTime: TimeMs,
    shuffleBytesRead: Bytes,
    shuffleRecordsRead: Long,
    shuffleLocalBytesRead: Bytes,
    shuffleRemoteBytesRead: Bytes,
    shuffleRemoteBytesReadToDisk: Bytes,
    inputBytesRead: Bytes,
    outputBytesWritten: Bytes,
    speculative: Boolean,
    successful: Boolean
) {

  def toJson(): JValue = {
    ("TaskID" -> taskId) ~
      ("StageId" -> stageId) ~
      ("JobID" -> jobId) ~
      ("ExecutorID" -> executorId) ~
      ("StartTime" -> startTime) ~
      ("EndTime" -> endTime) ~
      ("Duration" -> duration) ~
      ("ExecutorRunTime" -> executorRunTime) ~
      ("ExecutorCpuTime" -> executorCpuTime) ~
      ("GettingResultTime" -> gettingResultTime) ~
      ("SchedulerDelay" -> schedulerDelay) ~
      ("JvmGcTime" -> jvmGcTime) ~
      ("PeakExecutionMemory" -> peakExecutionMemory) ~
      ("ResultSize" -> resultSize) ~
      ("MemoryBytesSpilled" -> memoryBytesSpilled) ~
      ("DiskBytesSpilled" -> diskBytesSpilled) ~
      ("ShuffleWriteTime" -> shuffleWriteTime) ~
      ("ShuffleBytesWritten" -> shuffleBytesWritten) ~
      ("ShuffleRecordsWritten" -> shuffleRecordsWritten) ~
      ("ShuffleFetchWaitTime" -> shuffleFetchWaitTime) ~
      ("ShuffleBytesRead" -> shuffleBytesRead) ~
      ("ShuffleRecordsRead" -> shuffleRecordsRead) ~
      ("ShuffleLocalBytesRead" -> shuffleLocalBytesRead) ~
      ("ShuffleRemoteBytesRead" -> shuffleRemoteBytesRead) ~
      ("ShuffleRemoteBytesReadToDisk" -> shuffleRemoteBytesReadToDisk) ~
      ("InputBytesRead" -> inputBytesRead) ~
      ("OutputBytesWritten" -> outputBytesWritten) ~
      ("Speculative" -> speculative) ~
      ("Successful" -> successful)
  }
}
