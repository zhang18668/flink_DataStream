package hdpf.bean.sink

case class StopNumber(
                       var startTime: String,
                       var endTime: String,
                       var maxStopNum:Int,
                       var averStopNum:Int,
                       var roadId:Int
                     )
