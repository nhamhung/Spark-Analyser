package data.seafin.sparkles.optimizer

import data.seafin.sparkles.analyzer._
import data.seafin.sparkles.spark.{AppConfig, AppData}
import data.seafin.sparkles.util.ByteUnit
import data.seafin.sparkles.util.Common._

class ResourceSavingOptimizer extends Optimizer {
  import data.seafin.sparkles.analyzer.Recommendation._

  private val analyzers = List[Analyzer](
    new AppTimelineAnalyzer,
    new EnoughShufflePartitionsAnalyzer,
    new EnoughExecMemoryForCachingAnalyzer,
    new CoresPerExecutorAnalyzer,
    new ShuffleSpillAnalyzer,
    new StageSkewAnalyzer,
    new StageEfficiencyAnalyzer
  )

  override def optimize(app: AppData): Optimization = {
    val analyses = analyzers.map(_.analyze(app))
    val recs = analyses.map(_.recommend)

    val numPartitions = recs.getMax(Recommendation.minNumPartitions).get
      .min(Optimizer.MAX_PARTITION_NUMBER).max(Optimizer.DEFAULT_MIN_PARTITION_NUMBER)
    val numPartitionsPerCore = recs.getMax(Recommendation.numPartitionsPerCore).get
    val numCoresPerExecutor = recs.getMax(Recommendation.numCoresPerExecutor).get
    val recMinTotalMemoryRequired = recs.getMax(Recommendation.minTotalMemoryRequired)
    val maxTaskSkewFactors = recs.getMax(Recommendation.maxSkewFactor).get
    val totalCacheSize = recs.getMax(Recommendation.totalCacheSize).get

    val numExecutors =
      roundToNearestDivisible(numPartitions / numPartitionsPerCore / numCoresPerExecutor, 50).toInt.max(50)

    val currAppTotalMem = convertSizeUnit(
      app.appConfig.numExecutors.getOrElse(app.executors.size).toLong * app.appConfig.executorMemory.getOrElse(5120L),
      ByteUnit.MiB,
      ByteUnit.BYTE)

    val minTotalMemoryRequired = recMinTotalMemoryRequired match {
      case Some(amount) => amount
      case None         => currAppTotalMem
    }

    val memPerExecutor = convertSizeUnit(minTotalMemoryRequired / numExecutors,
        ByteUnit.BYTE,
        ByteUnit.MiB) + 1024L

    val overheadMemPerExecutor = (memPerExecutor * Optimizer.OVERHEAD_MEMORY_FRACTION).toInt
    val memoryFraction = Optimizer.DEFAULT_MEMORY_FRACTION
    var isAqeEnabled = false
    var isDynamicSkew = false
    var isCoalescePartition = false

    val isNumPartitionsHigh = numPartitions >= Optimizer.PARTITION_NUM_THRESHOLD
    val isHeavySkew = maxTaskSkewFactors >= Optimizer.SKEW_FACTOR_THRESHOLD

    if (isNumPartitionsHigh || isHeavySkew) {
      isAqeEnabled = true
      isDynamicSkew = true
      isCoalescePartition = true
    }

    // Try to adapt storageFraction to actual usage to increase execution memory peak
    val storageFraction = if (totalCacheSize > 0) {
      (totalCacheSize / (minTotalMemoryRequired * memoryFraction)).max(
        Optimizer.MIN_STORAGE_FRACTION
      )
    } else {
      Optimizer.MIN_STORAGE_FRACTION
    }

    val recommendedAppConfig = AppConfig(
      Some(numPartitions),
      Some(numExecutors),
      Some(numCoresPerExecutor),
      Some(memPerExecutor),
      Some(overheadMemPerExecutor),
      storageFraction,
      memoryFraction,
      isAqeEnabled,
      isDynamicSkew,
      isCoalescePartition
    )

    println("Original App Config")
    println(app.appConfig.toString())
    println("Recommended App Config")
    println(recommendedAppConfig.toString())

    val optimization = new Optimization(name, analyses, recommendedAppConfig)
    optimization
  }
}
