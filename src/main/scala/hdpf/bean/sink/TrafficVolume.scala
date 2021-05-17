package hdpf.bean.sink

case class TrafficVolume(
                       var startTime: String,
                       var endTime: String,
                       var carCount: Int,
                       var roadId: Int
                     )

