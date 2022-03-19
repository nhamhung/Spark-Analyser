package data.seafin.sparkles.spark

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.util.ByteUnit
import data.seafin.sparkles.util.Common._

/**
  * Interested Spark app configuration
  */
case class AppConfig(
    numPartitions: Option[Int],
    numExecutors: Option[Int],
    executorCores: Option[Int],
    executorMemory: Option[MB],
    executorMemoryOverhead: Option[MB],
    storageFraction: Double = 0.5,
    memoryFraction: Double = 0.6,
    aqeEnabled: Boolean = false,
    aqeCoalescePartitions: Boolean = false,
    aqeSkewJoinEnabled: Boolean = false
) {

  def toJson(): JValue = {
    ("NumPartitions" -> numPartitions) ~
      ("NumExecutors" -> numExecutors) ~
      ("ExecutorCores" -> executorCores) ~
      ("ExecutorMemory" -> executorMemory) ~
      ("ExecutorMemoryOverhead" -> executorMemoryOverhead) ~
      ("StorageFraction" -> storageFraction) ~
      ("MemoryFraction" -> memoryFraction) ~
      ("AqeEnabled" -> aqeEnabled) ~
      ("AqeCoalescePartitions" -> aqeCoalescePartitions) ~
      ("AqeSkewJoinEnabled" -> aqeSkewJoinEnabled) ~
      ("ConfigString" -> toString)
  }

  override def toString(): String = {
    val sb = new StringBuilder()
    numExecutors match {
      case Some(numExecutors) => sb ++= s"--num-executors ${numExecutors} \\\n"
      case None               =>
    }
    executorMemory match {
      case Some(executorMemory) =>
        sb ++= s"--executor-memory ${byteToString(executorMemory, ByteUnit.MiB)} \\\n"
      case None =>
    }
    executorMemoryOverhead match {
      case Some(executorMemoryOverhead) =>
        sb ++= s"--conf spark.executor.memoryOverhead=${byteToString(executorMemoryOverhead, ByteUnit.MiB)} \\\n"
      case None =>
    }
    executorCores match {
      case Some(executorCores) => sb ++= s"--conf spark.executor.cores=${executorCores} \\\n"
      case None                =>
    }
    numPartitions match {
      case Some(numPartitions) =>
        sb ++= s"--conf spark.sql.shuffle.partitions=${numPartitions} \\\n"
      case None =>
    }
    sb ++= f"--conf spark.memory.fraction=$memoryFraction%1.1f \\\n"
    sb ++= f"--conf spark.memory.storageFraction=$storageFraction%1.1f \\\n"
    sb ++= s"--conf spark.sql.adaptive.enabled=${aqeEnabled} \\\n"
    sb ++= s"--conf spark.sql.adaptive.skewJoin.enabled=${aqeSkewJoinEnabled} \\\n"
    sb ++= s"--conf spark.sql.adaptive.coalescePartition.enabled=${aqeCoalescePartitions}"
    sb.toString
  }
}
