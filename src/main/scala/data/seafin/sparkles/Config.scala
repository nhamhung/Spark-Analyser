package data.seafin.sparkles

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

/**
  * Store common app configs. [[analyzer.Analyzer]]' configs if needed will be extracted from
  * sparkConf.
  *
  * @param sparkConf
  * @param hadoopConf
  */
case class Config(sparkConf: SparkConf, hadoopConf: Configuration) {
  val appId = sparkConf.getOption(Config.APP_ID_KEY)

  def getLogDirFileSystemAndPath(): (FileSystem, Path) = {
    val dir = sparkConf.getOption(Config.LOG_DIR_KEY) match {
      case Some(dir) =>
        println("Using log location from user config")
        dir
      case None =>
        println("Using log location from spark")
        sparkConf
          .getOption(Config.SPARK_LOG_DIR_KEY)
          .getOrElse(throw new NoSuchElementException(Config.SPARK_LOG_DIR_KEY))
    }
    val uri = new URI(dir)
    (FileSystem.get(uri, hadoopConf), new Path(uri.getPath))
  }

  def getResultDirFileSystemAndPath(): (FileSystem, Path) = {
    val uri = sparkConf.getOption(Config.RESULT_DIR_KEY).map(new URI(_))
    uri match {
      case Some(uri) => (FileSystem.get(uri, hadoopConf), new Path(uri.getPath))
      case None      => throw new NoSuchElementException(Config.RESULT_DIR_KEY)
    }
  }
}

object Config {
  val APP_ID_KEY = "spark.sparkles.appid"
  val LOG_DIR_KEY = "spark.sparkles.log.dir"
  val RESULT_DIR_KEY = "spark.sparkles.result.dir"
  val SPARK_LOG_DIR_KEY = "spark.eventLog.dir"

  def apply(sparkConf: SparkConf, hadoopConf: Configuration): Config = {
    new Config(sparkConf, hadoopConf)
  }

  def apply(sparkConf: SparkConf): Config = {
    new Config(sparkConf, new Configuration())
  }
}
