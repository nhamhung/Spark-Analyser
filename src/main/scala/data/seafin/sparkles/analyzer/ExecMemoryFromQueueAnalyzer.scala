package data.seafin.sparkles.analyzer

import data.seafin.sparkles.spark.AppData

class ExecMemoryFromQueueAnalyzer extends Analyzer {
  override def analyze(app: AppData): Analysis = analysis
}
