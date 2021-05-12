package hdpf.bean.sink


case class StopDelay(
                       var startTime: String,
                       var endTime: String,
                       var maxdelay: Double,
                       var mindelay: Double,
                       var averdelay:Double,
                       var classfiy:Int
                     )
