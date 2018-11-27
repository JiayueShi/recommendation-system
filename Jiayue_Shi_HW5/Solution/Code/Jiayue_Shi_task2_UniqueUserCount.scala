import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object UniqueUserCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[2]").setAppName("HW5_task2")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    //    val lines = ssc.socketTextStream("localhost", 9999)
    val lines = ssc.socketTextStream(args(0), args(1).toInt)

    val result = lines.map(line => {
      val bin = line.toInt.toBinaryString
      val len1 = bin.length
      val len2 = bin.reverse.toLong.toString.length
      len1 - len2
    }).reduce((x1, x2) => math.max(x1, x2))
      .map(curNum => {
        math.pow(2, curNum).toInt
      })

    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}