package data.seafin.sparkles.util

import java.io.File

import org.apache.spark.SparkConf

import data.seafin.sparkles.{Config, SparklesTestSuite}

class EventLogReaderSuite extends SparklesTestSuite {
  test("globs event log file by appId") {

    withTempDir(dir => {
      // create dummy files
      val files: Seq[String] = Seq(
        "app_1",
        "app_1_1",
        "app_2",
        "app_3"
      )
      files.map(new File(dir, _).createNewFile())

      val conf = Config(new SparkConf().set(Config.LOG_DIR_KEY, dir.toString))
      val (fs, path) = conf.getLogDirFileSystemAndPath()

      val expected = s"file:${dir.toString}/app_1_1"
      val found = EventLogReader.getEventLogFile(fs, path, "app_1")
      assert(found.toString == expected)
    })
  }
}
