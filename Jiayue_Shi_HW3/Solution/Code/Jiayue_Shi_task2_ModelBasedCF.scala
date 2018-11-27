import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession

object ModelBasedCF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Task1")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    var spark = SparkSession.builder().master("local")
      .appName("spark_mllib")
      .config("spark.sql.warehouse.dir", "file:///")
      .getOrCreate()

    // Load and parse the data
    val file_name1 = args(0)
    val file_name2 = args(1)
    val outFileName = args(2)

//    var data1 = sc.textFile("data/video_small_num.csv")
    var test_data = sc.textFile(file_name2)
    var data1 = sc.textFile(file_name1)
    val data_header1 = data1.first()
    data1 = data1.filter(x => x != data_header1)
    sc.setCheckpointDir("targ")

//    var test_data = sc.textFile("data/video_small_testing_num.csv")
    val data_header2 = test_data.first()
    test_data = test_data.filter(x => x != data_header2)
    val test_ratings = test_data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)})

    var test_set = Set[(Int, Int)]()

    test_data.collect().foreach(line => {
      test_set += ((line.split(",")(0).toInt, line.split(",")(1).toInt))
    })

    val all_ratings = data1.map(_.split(',') match { case Array(user, item, rate, timestamp) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

//
    val ratings = all_ratings.filter(x => {
      val t = (x.user, x.product)
      !test_set.contains(t)
    })
//    val ratings = all_ratings.subtract(test_ratings)
//    println("train_data_num: " + ratings.collect().length)
    // Build the recommendation model using ALS
    val rank = 3
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.5, 1, 1)

    // Evaluate the model on rating data
    val usersProducts = test_ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val min = predictions.map(x => x._2).min()
    val max = predictions.map(x => x._2).max()
    val new_predictions = predictions
      .map{case ((user, product), rate) => ((user, product), 5 * (rate - min)/(max - min))}

    val ratesAndPreds = test_ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(new_predictions)
    val error_list = Array.ofDim[Int](5)
    var c = 0
    var MSE = ratesAndPreds.collect().map { case ((user, product), (r1, r2)) =>
      val err = math.abs(r1 - r2)
      if(err < 1){
        error_list(0) += 1
      }else if(err >= 1 && err < 2){
        error_list(1) += 1
      }else if(err >= 2 && err < 3){
        error_list(2) += 1
      }else if(err >= 3 && err < 4){
        error_list(3) += 1
      }else if(err >= 4){
        error_list(4) += 1
      }

      c += 1
      err * err
    }.sum

    MSE = MSE / c.toDouble

    val RMSE = math.sqrt(MSE)
    for(x <- error_list){
      println(x)
    }


    println(s"Mean Squared Error = $RMSE")

//    val outFileName = "data/Jiayue_Shi_ModelBasedCF.txt"
    val file = new File(outFileName)
    val bw = new BufferedWriter(new FileWriter(file))
    ratesAndPreds.collect().sorted.foreach{case ((user, product), (r1, r2)) => {
      val str = user.toInt.toString + "," + product.toInt.toString + "," + r2.toString + "\n"
      bw.write(str)
    }}
    bw.close()



  }
}
