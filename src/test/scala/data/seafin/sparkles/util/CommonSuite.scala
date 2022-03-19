package data.seafin.sparkles.util

import data.seafin.sparkles.SparklesTestSuite

import Common._

class CommonSuite extends SparklesTestSuite {
  test("gets byte from string") {
    val oneKbToB = byteFromString("1kb")
    val twoMbToKb = byteFromString("2mb", ByteUnit.KiB)
    val threeGbToMb = byteFromString("3g", ByteUnit.MiB)
    val fourMbToMb = byteFromString("4mb", ByteUnit.MiB)
    assert(oneKbToB == 1024L)
    assert(twoMbToKb == 2048L)
    assert(threeGbToMb == 3072L)
    assert(fourMbToMb == 4)
  }

  test("gets byte to string") {
    val oneGb = byteToString(1, ByteUnit.GiB)
    assert(oneGb == "1gb")
  }

  test("converts size unit") {
    val oneMillionBbToMb = convertSizeUnit(1 << 20, ByteUnit.BYTE, ByteUnit.MiB)
    val oneThousandMbToGb = convertSizeUnit(1 << 10, ByteUnit.MiB, ByteUnit.GiB)
    val oneBillionBbToGb = convertSizeUnit(1 << 30, ByteUnit.BYTE, ByteUnit.GiB)
    val oneMbToBb = convertSizeUnit(1, ByteUnit.MiB, ByteUnit.BYTE)
    val oneGbToMb = convertSizeUnit(1, ByteUnit.GiB, ByteUnit.MiB)
    val oneGbToBb = convertSizeUnit(1, ByteUnit.GiB, ByteUnit.BYTE)
    assert(oneMillionBbToMb == 1L)
    assert(oneThousandMbToGb == 1L)
    assert(oneBillionBbToGb == 1L)
    assert(oneMbToBb == 1 << 20)
    assert(oneGbToMb == 1 << 10)
    assert(oneGbToBb == 1 << 30)
  }

  test("round to nearest divisible 50") {
    assert(roundToNearestDivisible(50, 50) == 50)
    assert(roundToNearestDivisible(49, 50) == 0)
    assert(roundToNearestDivisible(51, 50) == 50)
    assert(roundToNearestDivisible(99, 50) == 50)
    assert(roundToNearestDivisible(100, 50) == 100)
  }

  test("math") {
    val l1 = List[Int](1, 1, 1, 1)
    val l2 = List[Int]()
    val l3 = List[Long](1, 2, 3, 4, 5, 6)
    assert(sum(l1) == Some(4))
    assert(min(l1) == Some(1))
    assert(max(l1) == Some(1))
    assert(mean(l1) == Some(1))
    assert(median(l1) == Some(1))
    assert(variance(l1) == Some(0))

    assert(sum(l2) == None)
    assert(min(l2) == None)
    assert(max(l2) == None)
    assert(mean(l2) == None)
    assert(median(l2) == None)
    assert(variance(l2) == None)

    assert(sum(l3) == Some(21L))
    assert(min(l3) == Some(1L))
    assert(max(l3) == Some(6L))
    assert(mean(l3) == Some(3.5))
    assert(median(l3) == Some(3.5))
    assert(variance(l3).map(_.toLong) == Some(2))
  }
}
