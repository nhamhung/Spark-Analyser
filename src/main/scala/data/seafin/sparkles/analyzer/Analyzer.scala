package data.seafin.sparkles.analyzer

import data.seafin.sparkles.spark.AppData

/**
  * Analyzes an aspect of [[spark.AppData]] and produces an analysis.
  */
trait Analyzer {
  protected val analysis = new Analysis(name)

  def name = this.getClass.getSimpleName.stripSuffix("$")

  def analyze(app: AppData): Analysis
}

object Analyzer {
  val TOP_K_METRICS = 5
  val HIGH_MEM_LEVEL_THRESHOLD = 200 // Gb
  val OPTIMAL_CORES_PER_EXECUTOR = 4
  val AQE_DEFAULT_ADVISORY_PARTITION_SIZE = 128
}
