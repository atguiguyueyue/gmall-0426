import com.alibaba.fastjson.JSON

object Test {
  def main(args: Array[String]): Unit = {
//    val eventLog: EventLog = new EventLog("1","2")
//    JSON.toJSONString(eventLog)
  }
  case class EventLog(mid:String,
                      uid:String)

}
