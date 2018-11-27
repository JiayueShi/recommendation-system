import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object ItemBasedCF {

  def pearson(v1: Array[Double], v2: Array[Double]): Double = {
    var l1 = ListBuffer[Double]()
    var l2 = ListBuffer[Double]()
    for (i <- v1.indices) {
      if (v1(i) != 0 && v2(i) != 0) {
        l1 += v1(i)
        l2 += v2(i)
      }
    }

    val avg1 = l1.sum / l1.size
    val avg2 = l2.sum / l2.size
    val bottom1 = math.sqrt(l1.map(x => (x - avg1) * (x - avg1)).sum)
    val bottom2 = math.sqrt(l2.map(x => (x - avg2) * (x - avg2)).sum)
    var sum = 0.0
    for (i <- l2.indices) {
      sum += (l1(i) - avg1) * (l2(i) - avg2)
    }
//    println(sum + " " + bottom1)
    var res = 0.0
    if (sum != 0) {
      res = sum / (bottom1 * bottom2)
    }
    res
  }

  def main(args: Array[String]): Unit = {
    val starttime = System.nanoTime()
    val conf = new SparkConf()
    conf.setAppName("Task1")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    var spark = SparkSession.builder().master("local")
      .appName("spark_mllib")
      .config("spark.sql.warehouse.dir", "file:///")
      .getOrCreate()


    val file_name1 = "data/video_small_num.csv"
    val file_name2 = "data/video_small_testing_num.csv"
    val file_name3 = "data/Jiayue_Shi_SimilarProducts_Jaccard.txt"
    val outFileName = "data/Jiayue_Shi_ItemBasedCF.txt"

//    val file_name1 = args(0)
//    val file_name2 = args(1)
//    val file_name3 = args(2)
//    val outFileName = args(3)


//    var data1 = sc.textFile(file_name1)

    var data1 = sc.textFile(file_name1)
    var data2 = sc.textFile(file_name3)
    var test_data = sc.textFile(file_name2)

    val data_header1 = data1.first()
    data1 = data1.filter(x => x != data_header1)


    //      var test_data = sc.textFile(file_name2)
    val data_header2 = test_data.first()
    test_data = test_data.filter(x => x != data_header2)

    val jac_similar = data2.map(_.split(',') match { case Array(item1, item2, similarity) =>
      Array(item1.toInt, item2.toInt)
    })

    val all_ratings = data1.map(_.split(',') match { case Array(user, item, rate, timestamp) =>
      Array(user.toInt, item.toInt, rate.toDouble)
    })
    val test_ratings = test_data.map(_.split(',') match { case Array(user, item, rate) =>
      Array(user.toInt, item.toInt, rate.toDouble)
    })

    var test_set = Set[(Int, Int)]()

    test_data.collect().foreach(line => {
      test_set += ((line.split(",")(0).toInt, line.split(",")(1).toInt))
    })

    val ratings = all_ratings.filter(x => {
      val t = (x(0).toInt, x(1).toInt)
      !test_set.contains(t)
    })

    var predict_items = Set[Int]()
    var test_map = Map[Int, ListBuffer[Int]]()

    test_ratings.collect().foreach(x => {
      val u = x(0).toInt
      val m = x(1).toInt
      if (test_map.contains(m)) {
        test_map(m) += u
      }
      else {
        val l = ListBuffer[Int]()
        l += u
        test_map += (m -> l)

      }
      predict_items += m
    })


    var target = predict_items.toList
    var jac_map = mutable.Map[Int, ListBuffer[Int]]()

    jac_similar.collect().foreach(x => {
      val m1 = x(0)
      val m2 = x(1)
      if(jac_map.contains(m1)){
        jac_map(m1) += m2
        jac_map(m1) = jac_map(m1).sorted.distinct
      }
      else{
        val l = ListBuffer[Int]()
        l += m2
        jac_map += (m1 -> l)
      }
    })



    val users = all_ratings.map(x => x(0).toInt).distinct().collect().sorted
    val movies = all_ratings.map(x => x(1).toInt).distinct().collect().sorted

    val users_length = users.length
    val movies_length = movies.length

    val rate_matrix = Array.ofDim[Double](movies_length, users_length)
    ratings.collect().foreach(x => {
      val user_id = x(0).toInt
      val movie_id = x(1).toInt
      rate_matrix(movie_id)(user_id) = x(2)
    })


    val test_num = target.size
    var predictions = ArrayBuffer[Array[Double]]()
    var pearson_map = Map[(Int,Int), Double]()

    var pearson_matrix = Array.ofDim[Double](movies_length, movies_length)

    jac_map.foreach(x => {
      val cur_m = x._1
      val similar_m = x._2
      for (elem <- similar_m) {
        if (cur_m < elem) {
          val vector1 = rate_matrix(cur_m)
          val vector2 = rate_matrix(elem)
          val p = pearson(vector1, vector2)
          if (p != 0) {
//            println(p)
            pearson_matrix(cur_m)(elem) = p
            pearson_matrix(elem)(cur_m) = p
          }
        }
      }


    })

    test_map.foreach(x => {
      val m = x._1
      val missing_rates = x._2.map(u => u)
      val missing_list = missing_rates.toList
      val m1 = m

      val vector1 = rate_matrix(m1)
      for (u <- missing_list) {
        var cur_pearson_sum = 0.0
        var cur_pearson_map = Map[Double, Int]()
        var cur_pearson_list = ListBuffer[Double]()
        var top = 0.0

        for (k <- 0 until movies_length) {
          if (rate_matrix(k)(u) != 0 && pearson_matrix(m1)(k) != 0) {
//            println(pearson_matrix(m1)(k))
            cur_pearson_list.append(pearson_matrix(m1)(k))
            cur_pearson_map += (pearson_matrix(m1)(k) -> k)
            cur_pearson_sum += math.abs(pearson_matrix(m1)(k))
            val vector2 = rate_matrix(k)
            top += pearson_matrix(m1)(k) * rate_matrix(k)(u)
          }
        }


        var predict_rate = 0.0
        if (cur_pearson_sum != 0){
          predict_rate = top / cur_pearson_sum
        }


//        if (cur_pearson_sum != 0) {
//          val sorted_p_list = cur_pearson_list.sortBy(math.abs(_)).reverse
//          val rate_num = sorted_p_list.size
//          if (rate_num > 7) {
//            cur_pearson_sum = 0.0
//            for (p <- 0 until 7) {
//              val p_movie = cur_pearson_map(sorted_p_list(p))
//              cur_pearson_sum += math.abs(pearson_matrix(m1)(p_movie))
//              top += rate_matrix(p_movie)(u) * pearson_matrix(m1)(p_movie)
//            }
//          }
//          predict_rate = top / cur_pearson_sum
//        }


        predictions += Array(u.toInt, m1.toInt, predict_rate)

      }

    })

    val movie_avg = Array.ofDim[Double](movies_length)
    for(i <- 0 until movies_length){
      var count = 0
      var cur_sum = 0.0
      for(j <- 0 until users_length){
        if(rate_matrix(i)(j) != 0){
          count += 1
          cur_sum += rate_matrix(i)(j)

        }
      }
      movie_avg(i) = cur_sum / count
    }

    val predict_rdd = sc.parallelize(predictions.toSeq).map {
      case Array(user, product, rate) =>
        ((user, product), rate)
    }

    val min = predict_rdd.map(x => x._2).min()
    val max = predict_rdd.map(x => x._2).max()
//    println("min:" + min + ", " + "max:" + max)

    val new_predictions = predict_rdd
      .map { case ((user, product), rate) => {
        var new_rate = 0.0
        if (rate < 0) {
          new_rate = 1
        }
        else if (rate > 4.8) {
          new_rate = 5
        }
        ((user, product), new_rate)
      }
      }
    val avg_predictions = new_predictions.map { case ((user, product), rate) => {

      var new_rate = rate
      if (new_rate == 0) {
        //        new_rate = user_avg_list(user.toInt)
        new_rate = movie_avg(product.toInt)
//        new_rate = 0.4 * movie_avg(product.toInt) + 0.6 * user_avg_list(user.toInt)
        if (new_rate < 0) {
          new_rate = 1
        } else if (new_rate > 5) {
          new_rate = 5
        }
      }
      ((user, product), new_rate)
    }
    }
    val ratesAndPreds = test_ratings.map {
      case Array(user, product, rate) => ((user, product), rate)
    }.join(avg_predictions)

    val error_list = Array.ofDim[Int](5)
    var c = 0

    var MSE = ratesAndPreds.collect().map { case ((user, product), (r1, r2)) =>
      val err = math.abs(r1 - r2)
//      println(r1 + ", " + r2)
      if (err < 1) {
        error_list(0) += 1
      } else if (err >= 1 && err < 2) {
        error_list(1) += 1
      } else if (err >= 2 && err < 3) {
        error_list(2) += 1
      } else if (err >= 3 && err < 4) {
        error_list(3) += 1
      } else if (err >= 4) {
        error_list(4) += 1
      }

      c += 1
      err * err
    }.sum

    MSE = MSE / c.toDouble

    val RMSE = math.sqrt(MSE)
    for (x <- error_list) {
      println(x)
    }


    println(s"Mean Squared Error = $RMSE")


    val file = new File(outFileName)
    val bw = new BufferedWriter(new FileWriter(file))
    ratesAndPreds.collect().sorted.foreach { case ((user, product), (r1, r2)) => {
      val str = user.toInt.toString + "," + product.toInt.toString + "," + r2.toString + "\n"
      bw.write(str)
    }
    }
    bw.close()


    val endtime = System.nanoTime()
    println(endtime - starttime)


  }



}
