package data.seafin.sparkles.util

import java.io.InputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

/**
  * Read event log file and decompress into input stream. All methods are static, this trait makes
  * it easier to mock.
  */
trait EventLogReader {
  def getEventLogAsStream(
      sparkConf: SparkConf,
      fs: FileSystem,
      logDir: Path,
      appId: Option[String]
  ): (InputStream, Path) = {
    val logFile = appId match {
      case Some(id) => getEventLogFile(fs, logDir, id)
      case None     => throw new IllegalArgumentException(s"Must provide appId to read event log.")
    }
    val codec = IOUtils.getCompressionCodecFromLogFile(sparkConf, logFile)
    val eventStream = codec.compressedInputStream(fs.open(logFile))
    (eventStream, logFile)
  }

  /**
    * Globs for file matching appId in event log dir.
    */
  def getEventLogFile(fs: FileSystem, logDir: Path, appId: String): Path = {
    val logFile = fs
      .globStatus(new Path(logDir, s"$appId{,_[0-9]*}"))
      .sortBy(_.getModificationTime)
      .lastOption
      .map(_.getPath)

    logFile match {
      case Some(path) => path
      case None       => throw new IllegalArgumentException(s"Can not find event log for $appId")
    }
  }
}

object EventLogReader extends EventLogReader {}
