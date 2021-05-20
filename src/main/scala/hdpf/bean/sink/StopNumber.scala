package hdpf.bean.sink

case class StopNumber(
                       var startTime: String,
                       var endTime: String,
                       var maxStopNum:Int,
                       var avgStopNum:Int,
                       var roadId:Int
                     )
