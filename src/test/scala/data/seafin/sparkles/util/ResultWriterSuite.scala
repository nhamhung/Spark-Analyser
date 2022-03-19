package data.seafin.sparkles.util

import org.apache.hadoop.fs.Path

import data.seafin.sparkles.SparklesTestSuite

class ResultWriterSuite extends SparklesTestSuite {
  test("generates correct path") {
    val dir = new Path("/some/dir")
    val appId = "app_1"
    val codec = "zstd"
    val resultPath = ResultWriter.getResultPath(dir, appId, Some(codec))
    assert(resultPath == "/some/dir/app_1.zstd")
  }

}
