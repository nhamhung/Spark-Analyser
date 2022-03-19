package data.seafin.sparkles.util

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.io.ZStdCompressionCodec

import data.seafin.sparkles.SparklesTestSuite

class IOUtilsSuite extends SparklesTestSuite {
  test("gets compression codec from file name") {
    val sparkConf = new SparkConf()
    val path = new Path("/sparkles/hello.zstd")
    val codec = IOUtils.getCompressionCodecFromLogFile(sparkConf, path)
    assert(codec.getClass == classOf[ZStdCompressionCodec])
  }

  test("sanitizes dir name") {
    assert(IOUtils.sanitizeDirName("UpperCase") == "uppercase")
    assert(IOUtils.sanitizeDirName("with.dot") == "with_dot")
    assert(IOUtils.sanitizeDirName("with space") == "with-space")
  }
}
