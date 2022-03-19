package data.seafin.sparkles

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.spark.SparkConf

class ConfigSuite extends SparklesTestSuite {
  test("gets local path from config") {
    val localDir = "file:///sparkles/logs"
    val conf = Config(new SparkConf().set(Config.LOG_DIR_KEY, localDir))

    val (fs, path) = conf.getLogDirFileSystemAndPath()
    assert(fs.getClass() == classOf[LocalFileSystem])
    assert(path.toString == "/sparkles/logs")
  }
}
