package data.seafin.sparkles.util

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Date, Locale}

/**
  * A collection of common types and converter utils.
  */
object Common {
  // Type aliases
  type JobId = Int
  type StageId = Int
  type TaskId = Long

  type TimeMs = Long
  type TimeNano = Long

  type Bytes = Long
  type MB = Long

  def nanoToMs(nano: TimeNano): TimeMs = nano / 1000000

  /**
    * Converts a string representation of size to unit.
    */
  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input, multiplier) =
      if (str.length() > 0 && str.charAt(0) == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  /**
    * Converts a string representation of size to bytes.
    */
  def byteFromString(str: String): Long = byteFromString(str, ByteUnit.BYTE)

  /**
    * Converts value of size unit to a string representation.
    */
  def byteToString(v: Long, unit: ByteUnit): String = {
    val inByte = unit.convertTo(v, ByteUnit.BYTE)
    val PB = 1L << 50
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit_) = {
      if (Math.abs(inByte) >= 1 * PB) {
        (inByte.asInstanceOf[Double] / PB, "pb")
      } else if (Math.abs(inByte) >= 1 * TB) {
        (inByte.asInstanceOf[Double] / TB, "tb")
      } else if (Math.abs(inByte) >= 1 * GB) {
        (inByte.asInstanceOf[Double] / GB, "gb")
      } else if (Math.abs(inByte) >= 1 * MB) {
        (inByte.asInstanceOf[Double] / MB, "mb")
      } else {
        (inByte.asInstanceOf[Double] / KB, "kb")
      }
    }
    "%.0f%s".formatLocal(Locale.US, value, unit_)
  }

  /**
    * Converts value of bytes to a string representation.
    */
  def byteToString(v: Long): String = byteToString(v, ByteUnit.BYTE)

  /**
    * Converts value of one unit to another unit.
    */
  def convertSizeUnit(v: Long, fromUnit: ByteUnit, toUnit: ByteUnit): Long = {
    fromUnit.convertTo(v, toUnit)
  }

  def roundToNearestDivisible(num: Double, divisible: Int): Long = {
    (divisible * (num / divisible).floor).toLong
  }

  def percentage(value: Double, total: Double): Double = value / total * 100

  // generic math
  def sum[T: Numeric](xs: Iterable[T]): Option[T] = {
    if (xs.size > 0) Some(xs.sum) else None
  }

  def max[T: Numeric](xs: Iterable[T]): Option[T] = {
    if (xs.size > 0) Some(xs.max) else None
  }

  def min[T: Numeric](xs: Iterable[T]): Option[T] = {
    if (xs.size > 0) Some(xs.min) else None
  }

  def mean[T: Numeric](xs: Iterable[T]): Option[Double] = {
    sum(xs).map(implicitly[Numeric[T]].toDouble(_) / xs.size)
  }

  def median[T: Numeric](xs: Seq[T]): Option[Double] = {
    if (xs.size > 0) {
      val sortedValues = xs.sorted.map(implicitly[Numeric[T]].toDouble(_))
      val middle = sortedValues.size / 2
      if (sortedValues.size % 2 == 0) {
        Some((sortedValues(middle - 1) + (sortedValues(middle))) / 2)
      } else {
        Some(sortedValues(middle))
      }
    } else None
  }

  def variance[T: Numeric](xs: Iterable[T]): Option[Double] = {
    mean(xs).map(m =>
      xs.map(implicitly[Numeric[T]].toDouble(_)).map(x => math.pow((x - m), 2)).sum / xs.size
    )
  }

  // time funcs

  val DF = new SimpleDateFormat("hh:mm:ss:SSS")

  def printDuration(ms: TimeMs): String = {
    "%02dm %02ds".format(
      TimeUnit.MILLISECONDS.toMinutes(ms),
      TimeUnit.MILLISECONDS.toSeconds(ms) -
        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(ms))
    )
  }

  def printHourMinute(ms: TimeMs): String = {
    val msToMinutes = ms % (60 * 60 * 1000)
    "%02dh %02dm".format(
      TimeUnit.MILLISECONDS.toHours(ms),
      TimeUnit.MILLISECONDS.toMinutes(msToMinutes)
    )
  }

  def printTime(ms: TimeMs): String = {
    DF.format(new Date(ms))
  }

}
