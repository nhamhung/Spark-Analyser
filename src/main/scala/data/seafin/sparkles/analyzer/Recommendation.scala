package data.seafin.sparkles.analyzer

import scala.collection.mutable

import data.seafin.sparkles.util.Common

sealed trait Recommendation {

  def apply[T](key: Key[T]): Option[T]

  def get[T](key: Key[T]): Option[T]

  def update[T](key: Key[T], value: T): Unit

  def ++(that: Recommendation): Recommendation
}

private class RecommendationImpl(private val inner: mutable.Map[Key[_], Option[_]])
    extends Recommendation {

  def apply[T](key: Key[T]): Option[T] = inner.apply(key).asInstanceOf[Option[T]]

  def get[T](key: Key[T]): Option[T] = apply(key)

  def update[T](key: Key[T], value: T): Unit = {
    inner(key) = Some(value)
  }

  def ++(that: Recommendation) = that match {
    case that: RecommendationImpl => new RecommendationImpl(this.inner ++ that.inner)
  }
}

final class Key[+T] {
  def ~>[A >: T](value: Option[A]): Recommendation = new RecommendationImpl(
    mutable.Map(this -> value)
  )
}

object Key {
  def apply[T] = new Key[T]
}

object Recommendation {
  val minNumPartitions = Key[Int]
  val maxNumPartitions = Key[Int]
  val numExecutors = Key[Int]
  val numCoresPerExecutor = Key[Int]
  val numPartitionsPerCore = Key[Int]
  val minTotalMemoryRequired = Key[Common.Bytes]
  val maxSkewFactor = Key[Int]
  val totalCacheSize = Key[Common.Bytes]

  def apply(): Recommendation = {
    minNumPartitions ~> None ++
      maxNumPartitions ~> None ++
      numExecutors ~> None ++
      numCoresPerExecutor ~> None ++
      numPartitionsPerCore ~> None ++
      minTotalMemoryRequired ~> None ++
      maxSkewFactor ~> None ++
      totalCacheSize ~> None
  }

  val rec = Recommendation()
  implicit class RecommendationList(val rs: List[Recommendation]) extends AnyVal {
    def getMax[T: Numeric](key: Key[T]): Option[T] = {
      Common.max(rs.map(_(key)).flatten)
    }

    def getMin[T: Numeric](key: Key[T]): Option[T] = {
      Common.min(rs.map(_(key)).flatten)
    }

    def getMean[T: Numeric](key: Key[T]): Option[Double] = {
      Common.mean(rs.map(_(key)).flatten)
    }
  }
}
