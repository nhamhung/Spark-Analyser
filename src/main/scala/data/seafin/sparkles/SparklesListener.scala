package data.seafin.sparkles

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, _}
import org.apache.spark.storage.RDDInfo

import data.seafin.sparkles.optimizer.Optimizer
import data.seafin.sparkles.spark._
import data.seafin.sparkles.util.ByteUnit
import data.seafin.sparkles.util.Common._

/**
  * Listen to Spark event stream and build [[spark.AppData]].
  */
class SparklesListener(sparkConf: SparkConf) extends SparkListener {
  import SparklesListener._

  val stageIdToJobId = new mutable.HashMap[StageId, JobId]
  val builder = new AppDataBuilder()

  /**
    * Collect basic info.
    */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    builder.addAppInfo(applicationStart.appId.getOrElse("NA"), applicationStart.appName)
    builder.addAppStartTime(applicationStart.time)
  }

  /**
    * Build [[spark.AppData]] and run [[optimizer.Optimizer]].
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    builder.addAppEndTime(applicationEnd.time)

    val appData = builder.build()
    val conf = Config(sparkConf)
    Optimizer.run(conf, appData)
  }

  private def getProperty(properties: Seq[(String, String)], key: String): Option[String] = {
    properties.find(_._1 == key).map(_._2)
  }

  /**
    * Collect [[spark.QueueInfo]] and [[spark.AppConfig]].
    */
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    val hadoopProperties =
      environmentUpdate.environmentDetails(HADOOP_PROPERTIES_KEY)
    val coresPerNode =
      getProperty(hadoopProperties, CORES_PER_NODE_KEY).map(_.toInt)
    val memoryPerNode =
      getProperty(hadoopProperties, MEMORY_PER_NODE_KEY).map(_.toLong)
    builder.addQueueInfo(new QueueInfo(coresPerNode, memoryPerNode))

    val sparkProperties =
      environmentUpdate.environmentDetails(SPARK_PROPERTIES_KEY)
    val numPartitions =
      getProperty(sparkProperties, NUM_PARTITIONS_KEY).map(_.toInt)
    val numExecutors = getProperty(sparkProperties, NUM_EXECUTORS_KEY).map(_.toInt)
    val executorCores =
      getProperty(sparkProperties, EXECUTOR_CORES_KEY).map(_.toInt)
    val executorMemory = getProperty(sparkProperties, EXECUTOR_MEMORY_KEY).map(
      byteFromString(_, ByteUnit.MiB)
    )
    val executorMemoryOverhead =
      getProperty(sparkProperties, EXECUTOR_MEMORY_OVERHEAD_KEY).map(
        byteFromString(_, ByteUnit.MiB)
      )
    val memoryFraction =
      getProperty(sparkProperties, MEMORY_FRACTION_KEY).getOrElse("0.6").toDouble
    val storageFraction =
      getProperty(sparkProperties, STORAGE_FRACTION_KEY).getOrElse("0.5").toDouble
    val aqeEnabled =
      getProperty(sparkProperties, AQE_ENABLED_KEY).getOrElse("false").toBoolean
    val aqeSkewJoinEnable = getProperty(sparkProperties, AQE_SKEW_JOIN_ENABLED_KEY)
      .getOrElse("false")
      .toBoolean
    val aqeCoalescePartitionEnable =
      getProperty(sparkProperties, AQE_COALESCE_PARTITION_ENABLED_KEY)
        .getOrElse("false")
        .toBoolean

    builder.addAppConfig(
      new AppConfig(
        numPartitions,
        numExecutors,
        executorCores,
        executorMemory,
        executorMemoryOverhead,
        memoryFraction,
        storageFraction,
        aqeEnabled,
        aqeSkewJoinEnable,
        aqeCoalescePartitionEnable
      )
    )
  }

  /**
    * Collects [[spark.ExecutorData]].
    */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    builder.addExecutorData(
      new PartialExecutorData(
        executorAdded.executorId,
        executorAdded.executorInfo.totalCores,
        executorAdded.time
      )
    )
  }

  /**
    * Collects [[spark.ExecutorData]]' endTime.
    */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    builder.addExecutorEndTime(executorRemoved.executorId, executorRemoved.time)
  }

  /**
    * Collects [[spark.JobData]]
    */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val sqlExecId = jobStart.properties.getProperty("spark.sql.execution.id")
    builder.addJobData(new PartialJobData(jobStart.jobId, sqlExecId, jobStart.time))

    jobStart.stageIds.foreach(stageId => {
      stageIdToJobId(stageId) = jobStart.jobId
    })
  }

  /**
    * Collects [[spark.JobData]]' endTime.
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    builder.addJobEndTime(jobEnd.jobId, jobEnd.time)
  }

  /**
    * Collects [[spark.StageData]]
    */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageInfo = stageSubmitted.stageInfo
    val startTime = stageInfo.submissionTime.getOrElse(0L)
    builder.addStageData(
      new PartialStageData(
        stageInfo.stageId,
        stageIdToJobId(stageInfo.stageId),
        stageInfo.name,
        stageInfo.numTasks,
        startTime,
        stageInfo.parentIds,
        stageInfo.rddInfos
      )
    )
  }

  /**
    * Collects [[spark.StageData]]' endTime, failureReason and rddInfos.
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val rddInfos: Seq[RDDInfo] = stageCompleted.stageInfo.rddInfos
    val endTime = stageInfo.completionTime.getOrElse(0L)
    builder.addStageEndTime(stageInfo.stageId, endTime)
    builder.addStageFailureReason(stageInfo.stageId, stageInfo.failureReason)
    builder.addStageRddInfo(stageInfo.stageId, rddInfos)
  }

  /**
    * Collects [[spark.TaskData]]
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    val gettingResultTime = {
      if (taskInfo.gettingResultTime == 0L) 0L
      else taskInfo.finishTime - taskInfo.gettingResultTime
    }
    val duration = taskInfo.finishTime - taskInfo.launchTime
    val schedulerDelay = math.max(
      0L,
      duration - taskMetrics.executorRunTime - taskMetrics.executorDeserializeTime -
        taskMetrics.resultSerializationTime - gettingResultTime
    )
    builder.addTaskData(
      new TaskData(
        taskInfo.taskId,
        stageIdToJobId(taskEnd.stageId),
        taskEnd.stageId,
        taskInfo.executorId,
        taskInfo.launchTime,
        taskInfo.finishTime,
        duration,
        taskMetrics.executorRunTime,
        nanoToMs(taskMetrics.executorCpuTime),
        gettingResultTime,
        schedulerDelay,
        taskMetrics.jvmGCTime,
        taskMetrics.peakExecutionMemory,
        taskMetrics.resultSize,
        taskMetrics.memoryBytesSpilled,
        taskMetrics.diskBytesSpilled,
        nanoToMs(taskMetrics.shuffleWriteMetrics.writeTime),
        taskMetrics.shuffleWriteMetrics.bytesWritten,
        taskMetrics.shuffleWriteMetrics.recordsWritten,
        taskMetrics.shuffleReadMetrics.fetchWaitTime,
        taskMetrics.shuffleReadMetrics.totalBytesRead,
        taskMetrics.shuffleReadMetrics.recordsRead,
        taskMetrics.shuffleReadMetrics.localBytesRead,
        taskMetrics.shuffleReadMetrics.remoteBytesRead,
        taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
        taskMetrics.inputMetrics.bytesRead,
        taskMetrics.outputMetrics.bytesWritten,
        taskInfo.speculative,
        taskInfo.successful
      )
    )
  }
}

object SparklesListener {
  // Keys to extract properties
  val HADOOP_PROPERTIES_KEY = "Hadoop Properties"
  val CORES_PER_NODE_KEY = "yarn.nodemanager.resource.cpu-vcores"
  val MEMORY_PER_NODE_KEY = "yarn.nodemanager.resource.memory-mb"
  val SPARK_PROPERTIES_KEY = "Spark Properties"
  val NUM_PARTITIONS_KEY = "spark.sql.shuffle.partitions"
  val NUM_EXECUTORS_KEY = "spark.executor.instances"
  val EXECUTOR_CORES_KEY = "spark.executor.cores"
  val EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val EXECUTOR_MEMORY_OVERHEAD_KEY = "spark.executor.memoryOverhead"
  val MEMORY_FRACTION_KEY = "spark.memory.fraction"
  val STORAGE_FRACTION_KEY = "spark.memory.storageFraction"
  val AQE_ENABLED_KEY = "spark.sql.adaptive.enabled"
  val AQE_SKEW_JOIN_ENABLED_KEY = "spark.sql.adaptive.skewJoin.enabled"
  val AQE_SKEWED_PARTITION_FACTOR_KEY = "spark.sql.adaptive.skewJoin.skewedPartitionFactor"
  val AQE_SKEWED_PARTITION_THRESHOLD_KEY =
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"
  val AQE_COALESCE_PARTITION_ENABLED_KEY = "spark.sql.adaptive.coalescePartition.enabled"
  val AQE_ADVISORY_PARTITION_SIZE_KEY = "spark.sql.adaptive.advisoryPartitionSizeInBytes"
  val AQE_AUTO_BROADCAST_JOIN_THRESHOLD_KEY = "spark.sql.adaptive.autoBroadcastJoinThreshold"
}
