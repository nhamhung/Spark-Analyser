package data.seafin.sparkles.spark

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import data.seafin.sparkles.util.Common._

/**
  * Queue info from Hadoop properties.
  */
case class QueueInfo(coresPerNode: Option[Int], memoryPerNode: Option[Bytes]) {
  def toJson(): JValue = {
    ("CoresPerNode" -> coresPerNode) ~
      ("MemoryPerNode" -> memoryPerNode)
  }
}

object QueueInfo {
  private implicit val format = DefaultFormats

  def fromJson(json: JValue): QueueInfo = {
    val coresPerNode = (json \ "Cores Per Node").extract[Int]
    val memoryPerNode = (json \ "Memory Per Node").extract[Long]
    new QueueInfo(Some(coresPerNode), Some(memoryPerNode))
  }
}
