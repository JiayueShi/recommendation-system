import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object task1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Task1")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    var spark=SparkSession.builder().master("local")
      .appName("spark_mllib")
      .config("spark.sql.warehouse.dir","file:///")
      .getOrCreate()

//    val df = spark.read.json("Toys_and_Games_5.json")
    val df = spark.read.json(args(0))

    val df1 = df.select("asin","overall")
//    df1.show()
    val result = df1.groupBy("asin").agg(avg("overall").as("rating_avg")).orderBy("asin")
//    result.show()
//    result.coalesce(1).write.option("header", "true").csv("Jiayue_Shi_result_task1")
    result.coalesce(1).write.option("header", "true").csv(args(1))


  }
}
