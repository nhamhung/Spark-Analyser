package data.seafin.sparkles.util

import java.io.{FileInputStream, FileOutputStream, IOException}
import java.util.Locale

import org.apache.commons.io
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.io.{
  CompressionCodec,
  LZ4CompressionCodec,
  LZFCompressionCodec,
  SnappyCompressionCodec,
  ZStdCompressionCodec
}

object IOUtils {
  def sanitizeDirName(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase(Locale.ROOT)
  }

  def createFile(fs: FileSystem, path: Path, allowEC: Boolean): FSDataOutputStream = {
    if (allowEC) {
      fs.create(path)
    } else {
      try {
        // Use reflection as this uses APIs only available in Hadoop 3
        val builderMethod = fs.getClass().getMethod("createFile", classOf[Path])
        // the builder api does not resolve relative paths, nor does it create parent dirs, while
        // the old api does.
        if (!fs.mkdirs(path.getParent())) {
          throw new IOException(s"Failed to create parents of $path")
        }
        val qualifiedPath = fs.makeQualified(path)
        val builder = builderMethod.invoke(fs, qualifiedPath)
        val builderCls = builder.getClass()
        // this may throw a NoSuchMethodException if the path is not on hdfs
        val replicateMethod = builderCls.getMethod("replicate")
        val buildMethod = builderCls.getMethod("build")
        val b2 = replicateMethod.invoke(builder)
        buildMethod.invoke(b2).asInstanceOf[FSDataOutputStream]
      } catch {
        case _: NoSuchMethodException =>
          // No createFile() method, we're using an older hdfs client, which doesn't give us control
          // over EC vs. replication.  Older hdfs doesn't have EC anyway, so just create a file with
          // old apis.
          fs.create(path)
      }
    }
  }

  /**
    * Loads CompressionCodec based on log file extension. Borrowed from
    * [[org.apache.spark.io.CompressionCodec]]
    */
  def getCompressionCodecFromLogFile(sparkConf: SparkConf, logFile: Path): CompressionCodec = {
    val logName = logFile.getName.stripSuffix(".inprogress")
    val codecName = logName.split("\\.").tail.last
    loadCompressionCodec(sparkConf, codecName)
  }

  private val compressionCodecMap = Map(
    "lz4" -> classOf[LZ4CompressionCodec].getName,
    "lzf" -> classOf[LZFCompressionCodec].getName,
    "snappy" -> classOf[SnappyCompressionCodec].getName,
    "zstd" -> classOf[ZStdCompressionCodec].getName
  )

  def loadCompressionCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    val codecClass = compressionCodecMap.getOrElse(codecName.toLowerCase, codecName)
    val classLoader =
      Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    val codec =
      try {
        val ctor = Class.forName(codecClass, true, classLoader).getConstructor(classOf[SparkConf])
        Some(ctor.newInstance(conf).asInstanceOf[CompressionCodec])
      } catch {
        case _: ClassNotFoundException | _: IllegalArgumentException => None
      }
    codec.getOrElse(throw new IllegalArgumentException(s"Codec [$codecName] is not available."))
  }

  def decompressFile(inFile: String): Unit = {
    val codecName = inFile.split("\\.").tail.last
    val codec = loadCompressionCodec(new SparkConf(), codecName)
    val outFile = inFile.stripSuffix(s".$codecName")
    val is = codec.compressedInputStream(new FileInputStream(inFile))
    val os = new FileOutputStream(outFile)
    io.IOUtils.copyLarge(is, os)
    os.close()
    is.close()
  }
}
