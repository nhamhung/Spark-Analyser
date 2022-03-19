package data.seafin.sparkles

import org.slf4j.{Logger, LoggerFactory}

/**
  * Utility trait for classes that want to log data. A class extending this trait gets a logger with
  * their name. Borrowed from [[org.apache.spark.internal.Logging]].
  */
trait Logging {
  @transient private var log_ : Logger = null

  // Get the logger name for this object
  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  // Get or create the logger for this object
  protected def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }
}
