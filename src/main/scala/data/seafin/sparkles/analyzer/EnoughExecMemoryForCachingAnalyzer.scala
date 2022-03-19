package data.seafin.sparkles.analyzer

import data.seafin.sparkles.spark.AppData
import data.seafin.sparkles.util.Common._

/**
  * Gets enough executor memory for cached data.
  */
class EnoughExecMemoryForCachingAnalyzer extends Analyzer {
  var totalCachedSize: Bytes = 0L

  override def analyze(app: AppData): Analysis = {
    analysis.addln(s"Stage-ID    RDD  MemSize  DiskSize  Partitions  --------Cached----------")
    analysis.addln(s"                                                Partitions | Storage    ")

    app.stages
      .foreach(stage => {
        stage.rddInfos
          .sortBy(_.id)
          .foreach(rdd => {
            if (rdd.isCached) {
              analysis.addln(
                f"${stage.stageId}%8s  ${rdd.id}%5s  ${byteToString(rdd.memSize)}%7s   ${byteToString(
                  rdd.diskSize
                )}%8s  ${rdd.numPartitions}%10s  ${rdd.numCachedPartitions}%10s   ${rdd.storageLevel}%-12s"
              )
              totalCachedSize += rdd.memSize + rdd.diskSize
            } else {
              analysis.addln(
                f"${stage.stageId}%8s  ${rdd.id}%5s  ${byteToString(
                  rdd.memSize
                )}%7s  ${byteToString(rdd.diskSize)}%8s  ${rdd.numPartitions}%10s  ${"-"}%10s   - "
              )
            }
          })
      })

    analysis.recommend(Recommendation.totalCacheSize) = totalCachedSize

    analysis.show
    analysis
  }
}
