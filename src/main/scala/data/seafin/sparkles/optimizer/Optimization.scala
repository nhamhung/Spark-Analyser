package data.seafin.sparkles.optimizer

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.analyzer.Analysis
import data.seafin.sparkles.spark.AppConfig

case class Optimization(name: String, analyses: List[Analysis], newAppConfig: AppConfig) {
  def toJson(): JValue = {
    ("Name" -> name) ~
      ("RecommendedAppConfig" -> newAppConfig.toJson) ~
      ("Analyses" -> JArray(analyses.map(_.toJson)))
  }
}
