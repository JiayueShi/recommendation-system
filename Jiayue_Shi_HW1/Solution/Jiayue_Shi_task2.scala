import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._



object task2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Task1")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var spark=SparkSession.builder().master("local")
      .appName("spark_mllib")
      .config("spark.sql.warehouse.dir","file:///")
      .getOrCreate()

//    val df_t = spark.read.json("Toys_and_Games_5.json")
//    val df_m = spark.read.json("metadata.json")
    val df_t = spark.read.json(args(0))
    val df_m = spark.read.json(args(1))

    val df1_m = df_m.select("brand","asin")
    val df1_t = df_t.select("asin","overall")

    val df2_m = df1_m.filter($"brand".isNotNull and $"brand".notEqual(" ") and $"brand".notEqual(""))

    val joined_df = df2_m.join(df1_t,Seq("asin"))


//    val df3 = df.select("asin","overall")
    //    df1.show()
    val result = joined_df.groupBy("brand").agg(avg("overall").as("rating_avg")).orderBy("brand")
    result.select("brand", "rating_avg").show()
//    result.coalesce(1).write.option("header", "true").mode("overwrite").csv("Jiayue_Shi_result_task2.csv")
    result.coalesce(1).write.option("header", "true").mode("overwrite").csv(args(2))


  }
}
