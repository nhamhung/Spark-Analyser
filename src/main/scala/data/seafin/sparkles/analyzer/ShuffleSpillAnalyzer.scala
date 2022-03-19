package data.seafin.sparkles.analyzer

import data.seafin.sparkles.spark.AppData
import data.seafin.sparkles.util.Common

/**
  * Checks for spillage to increase memory if needed.
  */
class ShuffleSpillAnalyzer extends Analyzer {
  override def analyze(app: AppData): Analysis = {

    val topMemSpill = app.stages
      .sortBy(_.taskAgg.map(_.memoryBytesSpilled.sum))
      .reverse
      .take(Analyzer.TOP_K_METRICS)
      .map(stage => (stage.stageId, stage.numTasks, stage.taskAgg.map(_.memoryBytesSpilled)))

    val topDiskSpill = app.stages
      .sortBy(_.taskAgg.map(_.diskBytesSpilled.sum))
      .reverse
      .take(Analyzer.TOP_K_METRICS)
      .map(stage => (stage.stageId, stage.numTasks, stage.taskAgg.map(_.diskBytesSpilled)))

    analysis.addTopStageSizes("Largest Memory Spill", topMemSpill)
    analysis.addTopStageSizes("Largest Disk Spill", topDiskSpill)

    // need enough memory to handle stage with most disk spill
    val mostDiskSpillStage =
      topDiskSpill.headOption.map(x => app.stages.filter(_.stageId == x._1).head)
    val minTotalMemRequired: Option[Common.Bytes] = mostDiskSpillStage match {
      case Some(stage) => {
        val memSpill = stage.taskAgg.map(_.memoryBytesSpilled.sum).getOrElse(0L)
        val diskSpill = stage.taskAgg.map(_.diskBytesSpilled.sum).getOrElse(0L)
        if (diskSpill > 0) {
          Some(diskSpill + memSpill)
        } else None
      }
      case None => None
    }

    minTotalMemRequired match {
      case Some(amount) => analysis.recommend(Recommendation.minTotalMemoryRequired) = amount
      case None         =>
    }

    analysis.show
    analysis
  }
}
