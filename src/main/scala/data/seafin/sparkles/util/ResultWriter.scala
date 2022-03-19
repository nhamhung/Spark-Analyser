package data.seafin.sparkles.util

import java.io._
import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import data.seafin.sparkles.Config

/**
  * Write app result to file. Borrowed from [[org.apache.spark.deploy.history.EventLogFileWriters]]
  */
class ResultWriter(conf: Config, appId: String) {

  private var hadoopDataStream: Option[FSDataOutputStream] = None
  private var writer: Option[PrintWriter] = None

  private val sparkConf = conf.sparkConf
  private val hadoopConf = conf.hadoopConf
  private val (fs, resultDir) = conf.getResultDirFileSystemAndPath()
  private val appId_ = appId

  private val compressionCodec =
    ResultWriter.CODEC_NAME.map(IOUtils.loadCompressionCodec(sparkConf, _))
  private val resultPath: String =
    ResultWriter.getResultPath(resultDir, appId_, ResultWriter.CODEC_NAME)
  private val inProgressPath = resultPath + ResultWriter.IN_PROGRESS

  private def requireResultDirAsDirectory(): Unit = {
    if (
      (fs.exists(resultDir) && !fs.getFileStatus(resultDir).isDirectory) || !fs.exists(resultDir)
    ) {
      // throw new IllegalArgumentException(s"Result directory $resultDir is not a directory.")
      println(s"Result directory $resultDir is not a directory. Making it one.")
      fs.mkdirs(resultDir)
    }
  }

  private def initLogFile(path: Path)(fnSetupWriter: OutputStream => PrintWriter): Unit = {
    if (ResultWriter.SHOULD_OVERWRITE && fs.delete(path, true)) {
      println(s"Result $path already exists. Overwriting...")
    }

    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"
    val uri = path.toUri

    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        hadoopDataStream = Some(IOUtils.createFile(fs, path, true))
        hadoopDataStream.get
      }

    try {
      val cstream = compressionCodec.map(_.compressedOutputStream(dstream)).getOrElse(dstream)
      val bstream = new BufferedOutputStream(cstream, ResultWriter.RESULT_FILE_OUTPUT_BUFFER_SIZE)
      fs.setPermission(path, ResultWriter.RESULT_FILE_PERMISSIONS)
      println(s"Writing to $path")
      writer = Some(fnSetupWriter(bstream))
    } catch {
      case e: Exception =>
        dstream.close()
        throw e
    }
  }

  private def writeLine(line: String, flushLogger: Boolean): Unit = {
    // scalastyle:off println
    writer.foreach(_.println(line))
    // scalastyle:on println
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(_.hflush())
    }
  }

  private def closeWriter(): Unit = {
    writer.foreach(_.close())
  }

  private def renameFile(src: Path, dest: Path, overwrite: Boolean): Unit = {
    if (fs.exists(dest)) {
      if (overwrite) {
        println(s"Result $dest already exists. Overwriting...")
        if (!fs.delete(dest, true)) {
          println(s"Error deleting $dest")
        }
      } else {
        throw new IOException(s"Target file already exists ($dest)")
      }
    }
    fs.rename(src, dest)
    // touch file to ensure modtime is current across those filesystems where rename()
    // does not set it but support setTimes() instead; it's a no-op on most object stores
    try {
      fs.setTimes(dest, System.currentTimeMillis(), -1)
    } catch {
      case e: Exception => println(s"failed to set time of $dest", e)
    }
  }

  /** initialize writer */
  def start(): Unit = {
    requireResultDirAsDirectory()

    initLogFile(new Path(inProgressPath)) { os =>
      new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))
    }
  }

  /** writes JSON to file */
  def write(json: String, flushLogger: Boolean = false): Unit = {
    writeLine(json, flushLogger)
  }

  /** stops writer - indicating the application has been completed */
  def stop(): Unit = {
    closeWriter()
    renameFile(new Path(inProgressPath), new Path(resultPath), ResultWriter.SHOULD_OVERWRITE)
  }

  /** starts, writes, and stops [[ResultWriter]] */
  def writeAll(json: String, flushLogger: Boolean = false): Unit = {
    this.start()
    this.write(json, flushLogger)
    this.stop()
  }
}

object ResultWriter {
  private val IN_PROGRESS = ".inprogress"
  private val SHOULD_OVERWRITE = true
  private val CODEC_NAME: Option[String] = None
  private val RESULT_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("660", 8).toShort)
  private val RESULT_FILE_OUTPUT_BUFFER_SIZE = 100

  def getResultPath(
      resultDir: Path,
      appId: String,
      compressionCodecName: Option[String] = None
  ): String = {
    val codec = compressionCodecName.map("." + _).getOrElse("")
    resultDir.toString.stripSuffix("/") + "/" + IOUtils.sanitizeDirName(appId) + codec
  }
}
