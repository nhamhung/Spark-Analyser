package data.seafin.sparkles.analyzer

import data.seafin.sparkles.spark.AppData
import data.seafin.sparkles.util.{ByteUnit, Common}

/**
  * Determines how many cores per executor.
  */
class CoresPerExecutorAnalyzer extends Analyzer {

  override def analyze(app: AppData): Analysis = {

    // Look at RunTime + CPU Time
    val topExecRunTime = app.stages
      .sortBy(_.taskAgg.map(_.executorRunTime.sum))
      .reverse
      .take(Analyzer.TOP_K_METRICS)
      .map(stage => (stage.stageId, stage.numTasks, stage.taskAgg.map(_.executorRunTime)))

    val topExecCpuTime = app.stages
      .sortBy(_.taskAgg.map(_.executorCpuTime.sum))
      .reverse
      .take(Analyzer.TOP_K_METRICS)
      .map(stage => (stage.stageId, stage.numTasks, stage.taskAgg.map(_.executorCpuTime)))

    // Look at data size and IO
    val topInputRead = app.stages
      .sortBy(_.taskAgg.map(_.inputBytesRead.sum))
      .reverse
      .take(Analyzer.TOP_K_METRICS)
      .map(stage => (stage.stageId, stage.numTasks, stage.taskAgg.map(_.inputBytesRead)))

    val topOutputWritten = app.stages
      .sortBy(_.taskAgg.map(_.outputBytesWritten.sum))
      .reverse
      .take(Analyzer.TOP_K_METRICS)
      .map(stage => (stage.stageId, stage.numTasks, stage.taskAgg.map(_.outputBytesWritten)))

    analysis.addTopStageDurations("Longest Executor Runtime", topExecRunTime)
    analysis.addTopStageDurations("Longest Executor CPU Time", topExecCpuTime)
    analysis.addTopStageSizes("Largest Input Read", topInputRead)
    analysis.addTopStageSizes("Largest Output Written", topOutputWritten)

    // Consider Queue Info, Data Size and Computation Size later
    val limitedClusterSize = true
    val smallDataSize =
      if (
        Common.convertSizeUnit(
          topInputRead.headOption.flatMap(_._3.map(_.sum)).getOrElse(0L),
          ByteUnit.BYTE,
          ByteUnit.GiB
        ) > Analyzer.HIGH_MEM_LEVEL_THRESHOLD
      ) {
        false
      } else {
        true
      }

    val numPartitionsPerCore = (limitedClusterSize, smallDataSize) match {
      case (true, true)   => 2
      case (true, false)  => 4
      case (false, true)  => 1
      case (false, false) => 3
    }

    // Most of the time, we should use 4-5 cores per executor.
    analysis.recommend(Recommendation.numCoresPerExecutor) = Analyzer.OPTIMAL_CORES_PER_EXECUTOR
    analysis.recommend(Recommendation.numPartitionsPerCore) = numPartitionsPerCore

    analysis.show
    analysis
  }
}
