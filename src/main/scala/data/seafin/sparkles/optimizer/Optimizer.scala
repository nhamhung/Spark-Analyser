package data.seafin.sparkles.optimizer

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.Config
import data.seafin.sparkles.spark.AppData
import data.seafin.sparkles.util.ResultWriter

/**
  * Optimize Spark application based on some objectives using result of [[analyzer.Analyzer]].
  */
trait Optimizer {
  def name = this.getClass.getSimpleName.stripSuffix("$")

  def optimize(appData: AppData): Optimization
}

object Optimizer {
  private val OPTIMIZER_KEY = "spark.sparkles.optimizer"

  val PARTITION_NUM_THRESHOLD = 2000
  val SKEW_FACTOR_THRESHOLD = 10
  val DEFAULT_MEMORY_FRACTION = 0.6
  val DEFAULT_STORAGE_FRACTION = 0.5
  val MIN_STORAGE_FRACTION = 0.2
  val OVERHEAD_MEMORY_FRACTION = 0.1
  val DEFAULT_MIN_PARTITION_NUMBER = 200
  val MAX_PARTITION_NUMBER = 8000

  private val optimizerMap = Map(
    "RuntimeSavingOptimizer" -> classOf[RuntimeSavingOptimizer].getName,
    "ResourceSavingOptimizer" -> classOf[ResourceSavingOptimizer].getName
  )

  def apply(name: String): Optimizer = {
    optimizerMap.get(name) match {
      case Some(optClass) => {
        val classLoader = {
          Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
        }
        val opt = {
          val ctor = Class.forName(optClass, true, classLoader).getConstructor()
          Some(ctor.newInstance().asInstanceOf[Optimizer])
        }
        opt.getOrElse(throw new IllegalArgumentException(s"Cannot load optimizer [$name]."))
      }
      case None => throw new IllegalArgumentException(s"Unknown optimizer [$name].")
    }
  }

  def run(conf: Config, appData: AppData): Unit = {
    val optimizerName =
      conf.sparkConf.getOption(OPTIMIZER_KEY).getOrElse("RuntimeSavingOptimizer")
    val optimizer = Optimizer(optimizerName)
    val optimization = optimizer.optimize(appData)

    val resultWriter = new ResultWriter(conf, appData.appId)
    val result: JValue = {
      ("Optimization" -> optimization.toJson) ~
        ("AppData" -> appData.toJson)
    }
    resultWriter.writeAll(compactRender(result))
  }
}
