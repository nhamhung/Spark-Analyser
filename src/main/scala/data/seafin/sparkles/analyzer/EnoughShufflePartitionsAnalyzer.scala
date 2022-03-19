package data.seafin.sparkles.analyzer

import data.seafin.sparkles.spark.AppData
import data.seafin.sparkles.util.ByteUnit
import data.seafin.sparkles.util.Common._

/**
  * Gets enough partitions so size is from 200 to 500 MB.
  */
class EnoughShufflePartitionsAnalyzer extends Analyzer {
  override def analyze(app: AppData): Analysis = {
    val topShuffleRead = app.stages
      .sortBy(_.taskAgg.map(_.shuffleBytesRead.sum))
      .reverse
      .take(Analyzer.TOP_K_METRICS)
      .map(stage => (stage.stageId, stage.numTasks, stage.taskAgg.map(_.shuffleBytesRead)))

    analysis.addTopStageSizes("Heaviest Shuffle Read", topShuffleRead)

    val largestShuffleRead = topShuffleRead.headOption.flatMap(_._3.map(_.sum)).getOrElse(0L)
    val largestShuffleReadInMb = convertSizeUnit(largestShuffleRead, ByteUnit.BYTE, ByteUnit.MiB)

    val maxNumPartitions = roundToNearestDivisible(largestShuffleReadInMb / 200, 50).toInt
    val minNumPartitions = roundToNearestDivisible(largestShuffleReadInMb / 500, 50).toInt

    analysis.recommend(Recommendation.minNumPartitions) = minNumPartitions
    analysis.recommend(Recommendation.maxNumPartitions) = maxNumPartitions

    analysis.show
    analysis
  }
}
