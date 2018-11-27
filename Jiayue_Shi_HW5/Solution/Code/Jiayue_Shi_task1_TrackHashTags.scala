import org.apache.log4j.{Level, Logger}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf


object TrackHashTags {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

//    var Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    val consumerKey = "SxbNIoG7GiilAwbylHEtYJgNt"
    val consumerSecret = "HvdhruYesRozcQbAy7oJYokObwtdvbAfQrgIbP4ykKJeajN0X6"
    val accessToken = "986059857311641601-xy14aoe6esJVtCePE2jVvZjfFLHCagd"
    val accessTokenSecret = "DaTpBqFXHISgfELAsUHyUs1t5iOA5fGbIp24D6Drqcoez"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")


    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#"))).window(Seconds(120))

    val topCounts2 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(2))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts2.foreachRDD(rdd => {
      val topList = rdd.take(5)
//      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s, count: %s".format(tag, count))}
    })



    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println