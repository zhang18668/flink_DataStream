package hdpf.bean.sink


case class StopDelay(
                       var startTime: String,
                       var endTime: String,
                       var maxDelay: Double,
                       var minDelay: Double,
                       var avgDelay:Double,
                       var roadId:Int
                     )
