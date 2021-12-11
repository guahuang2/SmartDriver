import scala.reflect.runtime.universe._


case class KafkaTaxiRecord(
                              area:Int,
                              month: Int,
                              day: Int,
                              hour: Int,
                              minute: Int,
                              ehour: Int,
                              eminute: Int,
                              tripm: Long,
                              fare: Long,
                              tips:Long,total:Long)