package hdpf.bean.sink

case class QueueLength(
  var timestamp:String,
  var queueLength:Double,
  var classfiy:Int
)
