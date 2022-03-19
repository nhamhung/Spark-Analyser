package data.seafin.sparkles.spark

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.util.Common._

/**
  * Aggregation of multiple task metrics.
  */
case class TaskDataAggregation(
    duration: AggregateValue[TimeMs],
    executorRunTime: AggregateValue[TimeMs],
    executorCpuTime: AggregateValue[TimeMs],
    gettingResultTime: AggregateValue[TimeMs],
    schedulerDelay: AggregateValue[TimeMs],
    jvmGcTime: AggregateValue[TimeMs],
    peakExecutionMemory: AggregateValue[Bytes],
    resultSize: AggregateValue[Bytes],
    memoryBytesSpilled: AggregateValue[Bytes],
    diskBytesSpilled: AggregateValue[Bytes],
    shuffleWriteTime: AggregateValue[TimeMs],
    shuffleBytesWritten: AggregateValue[Bytes],
    shuffleRecordsWritten: AggregateValue[Long],
    shuffleFetchWaitTime: AggregateValue[TimeMs],
    shuffleBytesRead: AggregateValue[Bytes],
    shuffleRecordsRead: AggregateValue[Long],
    shuffleLocalBytesRead: AggregateValue[Bytes],
    shuffleRemoteBytesRead: AggregateValue[Bytes],
    shuffleRemoteBytesReadToDisk: AggregateValue[Bytes],
    inputBytesRead: AggregateValue[Bytes],
    outputBytesWritten: AggregateValue[Bytes]
) {
  def toJson(): JValue = {
    ("Duration" -> duration.toJson) ~
      ("ExecutorRunTime" -> executorRunTime.toJson) ~
      ("ExecutorCpuTime" -> executorCpuTime.toJson) ~
      ("GettingResultTime" -> gettingResultTime.toJson) ~
      ("SchedulerDelay" -> schedulerDelay.toJson) ~
      ("JvmGcTime" -> jvmGcTime.toJson) ~
      ("PeakExecutionMemory" -> peakExecutionMemory.toJson) ~
      ("ResultSize" -> resultSize.toJson) ~
      ("MemoryBytesSpilled" -> memoryBytesSpilled.toJson) ~
      ("DiskBytesSpilled" -> diskBytesSpilled.toJson) ~
      ("ShuffleWriteTime" -> shuffleWriteTime.toJson) ~
      ("ShuffleBytesWritten" -> shuffleBytesWritten.toJson) ~
      ("ShuffleRecordsWritten" -> shuffleRecordsWritten.toJson) ~
      ("ShuffleFetchWaitTime" -> shuffleFetchWaitTime.toJson) ~
      ("ShuffleBytesRead" -> shuffleBytesRead.toJson) ~
      ("ShuffleRecordsRead" -> shuffleRecordsRead.toJson) ~
      ("ShuffleLocalBytesRead" -> shuffleLocalBytesRead.toJson) ~
      ("ShuffleRemoteBytesRead" -> shuffleRemoteBytesRead.toJson) ~
      ("ShuffleRemoteBytesReadToDisk" -> shuffleRemoteBytesReadToDisk.toJson) ~
      ("InputBytesRead" -> inputBytesRead.toJson) ~
      ("OutputBytesWritten" -> outputBytesWritten.toJson)
  }
}

object TaskDataAggregation {
  def apply(tasks: List[TaskData]): Option[TaskDataAggregation] = tasks match {
    case Nil => None
    case tasks =>
      Some(
        new TaskDataAggregation(
          AggregateValue(tasks.map(_.duration)),
          AggregateValue(tasks.map(_.executorRunTime)),
          AggregateValue(tasks.map(_.executorCpuTime)),
          AggregateValue(tasks.map(_.gettingResultTime)),
          AggregateValue(tasks.map(_.schedulerDelay)),
          AggregateValue(tasks.map(_.jvmGcTime)),
          AggregateValue(tasks.map(_.peakExecutionMemory)),
          AggregateValue(tasks.map(_.resultSize)),
          AggregateValue(tasks.map(_.memoryBytesSpilled)),
          AggregateValue(tasks.map(_.diskBytesSpilled)),
          AggregateValue(tasks.map(_.shuffleWriteTime)),
          AggregateValue(tasks.map(_.shuffleBytesWritten)),
          AggregateValue(tasks.map(_.shuffleRecordsWritten)),
          AggregateValue(tasks.map(_.shuffleFetchWaitTime)),
          AggregateValue(tasks.map(_.shuffleBytesRead)),
          AggregateValue(tasks.map(_.shuffleRecordsRead)),
          AggregateValue(tasks.map(_.shuffleLocalBytesRead)),
          AggregateValue(tasks.map(_.shuffleRemoteBytesRead)),
          AggregateValue(tasks.map(_.shuffleRemoteBytesReadToDisk)),
          AggregateValue(tasks.map(_.inputBytesRead)),
          AggregateValue(tasks.map(_.outputBytesWritten))
        )
      )
  }
}

/**
  * Stores aggregate values.
  */
case class AggregateValue[T <% JValue: Numeric](
    sum: T,
    min: T,
    max: T,
    mean: Double,
    median: Double,
    variance: Double
) {

  def toJson(): JValue = {
    ("Sum" -> sum) ~
      ("Min" -> min) ~
      ("Max" -> max) ~
      ("Median" -> mean) ~
      ("Variance" -> variance)
  }

  override def toString(): String = {
    s"""{
       | "sum": ${sum}
       | "min": ${min},
       | "max": ${max},
       | "mean": ${mean},
       | "median": ${median},
       | "variance": ${variance}
       }""".stripMargin
  }
}

object AggregateValue {
  def apply[T <% JValue: Numeric](xs: List[T]): AggregateValue[T] = {
    // makes sure that xs is not Nil
    new AggregateValue(
      sum(xs).get,
      min(xs).get,
      max(xs).get,
      mean(xs).get,
      median(xs).get,
      variance(xs).get
    )
  }
}
