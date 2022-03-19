package data.seafin.sparkles.analyzer

import data.seafin.sparkles.spark.AppData
import data.seafin.sparkles.util.Common._

/**
  * Shows statistics for stage efficiency.
  */
class StageEfficiencyAnalyzer extends Analyzer {

  override def analyze(app: AppData): Analysis = {
    val appConfig = app.appConfig
    val totalCores = appConfig.numExecutors.getOrElse(0) * appConfig.executorCores.getOrElse(0)
    val totalStageDuration = app.stages.map(_.duration).sum
    val oneCoreDuration = totalStageDuration * totalCores // duration if have only 1 core
    val totalStageRunTime = app.stages.map(_.taskAgg.map(_.executorRunTime.sum).getOrElse(0L)).sum
    val totalIO = app.stages
      .flatMap(
        _.taskAgg.map(agg =>
          agg.inputBytesRead.sum + agg.outputBytesWritten.sum + agg.shuffleBytesRead.sum + agg.shuffleBytesWritten.sum
        )
      )
      .sum

    analysis.addln(s"App Duration: ${printDuration(app.duration)}")
    analysis.addln(s"Total Cores: $totalCores")
    analysis.addln(s"Total Stages' Duration: ${printDuration(totalStageDuration)}")
    analysis.addln(s"OneCore Duration: ${printDuration(oneCoreDuration)}")
    analysis.addln(s"Total Runtime: ${printDuration(totalStageRunTime)}")
    analysis.addln(s"Total IO: ${byteToString(totalIO)}\n")

    analysis.addln("Per Stage Utilization")
    analysis.addln(
      s"Stage-ID      Stage  Executor   Task     IO%    Input     Output    ----Shuffle-----    -WallClockTime-    --OneCoreComputeHours---   MaxTaskMem"
    )
    analysis.addln(
      s"          Duration%  Runtime%   Count                               Input  |  Output    Measured | Ideal   Available| Used%|Wasted%                                  "
    )

    app.stages
      .foreach(stage => {
        stage.taskAgg match {
          case Some(taskAgg) => {
            val stageDuration = stage.duration
            val stageTaskCount = stage.numTasks
            val stageInputBytesRead = taskAgg.inputBytesRead.sum
            val stageOutputBytesWritten = taskAgg.outputBytesWritten.sum
            val stageShuffleBytesRead = taskAgg.shuffleBytesRead.sum
            val stageShuffleBytesWritten = taskAgg.shuffleBytesWritten.sum
            val stageTaskPeakMemoryMax = taskAgg.peakExecutionMemory.max
            val stageUsed = taskAgg.executorRunTime.sum
            val stageOneCoreDuration = stageDuration * totalCores
            val stageWasted = stageOneCoreDuration - stageUsed
            val stageIO =
              stageInputBytesRead + stageOutputBytesWritten + stageShuffleBytesRead + stageShuffleBytesWritten

            val stageDurationPercent = percentage(stageDuration, totalStageDuration)
            val stageUsedPercent = percentage(stageUsed, stageOneCoreDuration)
            val stageWastedPercent = percentage(stageWasted, stageOneCoreDuration)
            val stageTaskRuntimePercent = percentage(stageUsed, totalStageRunTime)
            val stageIOPercent = percentage(stageIO, totalIO)

            // Ideally, if parallelism is maximised, then totalExecutorRuntime is like OneCoreComputeHours (Double Check Intuition)
            val idealWallClock = stageUsed / totalCores

            analysis.addln(
              f"${stage.stageId}%8s      "
                + f"${stageDurationPercent}%5.2f   " // Stage's ms percent
                + f"${stageTaskRuntimePercent}%5.2f   " // Stage's executorRuntime percent
                + f"${stageTaskCount}%7s  "
                + f"${stageIOPercent}%5.1f  "
                + f"${byteToString(stageInputBytesRead)}%8s  "
                + f"${byteToString(stageOutputBytesWritten)}%8s  "
                + f"${byteToString(stageShuffleBytesRead)}%8s  "
                + f"${byteToString(stageShuffleBytesWritten)}%8s    "
                + f"${printDuration(stageDuration)}   " // Stage's actual duration
                + f"${printDuration(idealWallClock)} " // Stage's executorRuntime / totalCores
                + f"${printHourMinute(stageOneCoreDuration)}%10s  "
                + f"${stageUsedPercent}%5.1f  "
                + f"${stageWastedPercent}%5.1f"
                + f"${byteToString(stageTaskPeakMemoryMax)}%10s "
            )
          }
          case None =>
        }
      })

    analysis.show
    analysis
  }

}
