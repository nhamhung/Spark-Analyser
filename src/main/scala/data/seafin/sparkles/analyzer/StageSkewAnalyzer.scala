package data.seafin.sparkles.analyzer

import data.seafin.sparkles.spark.AppData
import data.seafin.sparkles.util.Common._

class StageSkewAnalyzer extends Analyzer {
  var maxTaskSkewFactor = 0

  override def analyze(app: AppData): Analysis = {
    analysis.addln("Per Stage Skewness Degree")
    analysis.addln(
      s" Stage-ID   MaxTime   MaxMem       Task     HighestTask     Mem      |* ShuffleWrite%   ReadFetch%   GC%   Scheduler%  *|"
    )
    analysis.addln(
      s"                                   Skew     Fraction        Skew                                                         "
    )

    app.stages
      .foreach(stage => {
        stage.taskAgg match {
          case Some(taskAgg) => {
            val stageTotalExecutorTime = taskAgg.executorRunTime.sum
            val stageShuffleWriteTime = taskAgg.shuffleWriteTime.sum
            val stageShuffleWritePercent = percentage(stageShuffleWriteTime, stageTotalExecutorTime)

            val stageShuffleFetchWaitTime = taskAgg.shuffleFetchWaitTime.sum
            val stageReadFetchPercent =
              percentage(stageShuffleFetchWaitTime, stageTotalExecutorTime)

            val stageGCTime = taskAgg.jvmGcTime.sum
            val stageGCPercent = stageGCTime / stageTotalExecutorTime

            val schedulerDelayTime = taskAgg.schedulerDelay.sum
            val stageSchedulerDelayPercent = percentage(schedulerDelayTime, stageTotalExecutorTime)

            // Get Task Execution Memory Skew
            val stageMaxTaskMem = taskAgg.peakExecutionMemory.max
            val stageMedianTaskMem = taskAgg.peakExecutionMemory.median
            val stageTaskMemSkew = if (stageMedianTaskMem > 0) {
              stageMaxTaskMem.toFloat / stageMedianTaskMem
            } else {
              0
            }

            // Get Task Execution Time Skew
            val stageMaxTaskTime = taskAgg.executorRunTime.max
            val stageMedianTaskTime = taskAgg.executorRunTime.median
            val stageTaskTimeSkew = if (stageMedianTaskTime > 0) {
              stageMaxTaskTime.toFloat / stageMedianTaskTime
            } else {
              0
            }

            if (stageTaskTimeSkew > maxTaskSkewFactor) {
              maxTaskSkewFactor = stageTaskTimeSkew.toInt
            }

            // Get Max Task Execution Time / Stage Duration
            val stageDuration = stage.duration
            val stageTaskTimeOverDuration = if (stageDuration > 0) {
              stageMaxTaskTime.toFloat / stageDuration
            } else {
              0
            }

            analysis.addln(
              f"${stage.stageId}%8s   "
                + f"${printDuration(stageMaxTaskTime)}%8s"
                + f"${byteToString(stageMaxTaskMem)}%8s"
                + f"${stageTaskTimeSkew}%11.1f   "
                + f"${stageTaskTimeOverDuration}%7.2f   "
                + f"${stageTaskMemSkew}%13.1f   "
                + f"${stageShuffleWritePercent}%10.1f  "
                + f"${stageReadFetchPercent}%15.1f  "
                + f"${stageGCPercent}%8.1f  "
                + f"${stageSchedulerDelayPercent}%7.1f  "
            )
          }
          case None =>
        }
      })

    analysis.recommend(Recommendation.maxSkewFactor) = maxTaskSkewFactor

    analysis.show
    analysis
  }

}
