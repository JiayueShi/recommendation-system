import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}
import scala.math.Ordering.Implicits._
import java.io._

object JaccardLSH {

  def h1(x: Int, m: Int): Int = {
    (3 * x + 13) % m
  }

  def h2(x: Int, m: Int): Int = {
    (7 * x + 11) % m
  }

  def jaccard(p1: Int, p2: Int, char_matrix: Array[Array[Int]]): Double = {
    var intersection = 0
    var union = 0

    val users_length = char_matrix.length
    //    val movies_length = char_matrix(0).length
    for (x <- 0 until users_length) {
      val cur_user_of_p1 = char_matrix(x)(p1)
      val cur_user_of_p2 = char_matrix(x)(p2)

      if (cur_user_of_p1 == 1 || cur_user_of_p2 == 1) {
        union += 1
      }
      if (cur_user_of_p1 == 1 && cur_user_of_p2 == 1) {
        intersection += 1
      }
    }
    //    println(intersection + " " + union + " " + intersection.toDouble / union.toDouble)
    intersection.toDouble / union.toDouble

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

//    val file_name1 = "data/video_small_num.csv"
//    val file_name2 = "data/video_small_ground_truth_jaccard.csv"
//    val outFileName = "data/Jiayue_Shi_SimilarProducts_Jaccard.txt"
    val file_name1 = args(0)
//    val file_name2 = args(1)
    val outFileName = args(1)

    var data = sc.textFile(file_name1)
    val data_header = data.first()
    data = data.filter(x => x != data_header)

    val pairs = data.map(line => line.split(",")(0) + " " + line.split(",")(1))
    //    pairs.take(5).foreach(println)

    val users = data.map(line => line.split(",")(0).toInt).distinct().collect().sorted
    val movies = data.map(line => line.split(",")(1).toInt).distinct().collect().sorted

    //    movies.take(5).foreach(println)
    //    users.take(5).foreach(println)
    val users_length = users.length
    val movies_length = movies.length
    //    println(users_length + " " + movies_length)

    var users_dict = Map[Int, Int]()
    var movies_dict = Map[Int, Int]()
    var movies_dict_reverse = Map[Int, Int]()

    var count = 0
    users.foreach(x => {
      users_dict += (x -> count)
      count += 1
    })

    count = 0
    movies.foreach(x => {
      movies_dict += (x -> count)
      count += 1
    })

    movies_dict.foreach(x => {
      val k = x._1
      val v = x._2
      movies_dict_reverse += (v -> k)

    })

    val char_matrix = Array.ofDim[Int](users_length, movies_length)

    pairs.collect().foreach(x => {
      val user_id = x.split(" ")(0).toInt
      val movie_id = x.split(" ")(1).toInt
      //      println(user_id + " " + movie_id )
      //      println(users_dict(user_id) + " " + movies_dict(movie_id))
      char_matrix(users_dict(user_id))(movies_dict(movie_id)) = 1
    })

    //    println(char_matrix(9)(977))
    val hash_num = 2

    //    val sig_matrix = Array.ofDim[Int](hash_num, movies_length)
    val sig_matrix = Array.fill(hash_num, movies_length)(Int.MaxValue)


    val h_res = Array.ofDim[Int](hash_num, users_length)

    for (x <- 0 until users_length) {
      h_res(0)(x) = h1(x, users_length)
      h_res(1)(x) = h2(x, users_length)
    }


    for (i <- 0 until users_length) {
      for (j <- 0 until movies_length) {
        if (char_matrix(i)(j) == 1) {
          for (k <- 0 until hash_num) {
            if (sig_matrix(k)(j) > h_res(k)(i)) {
              sig_matrix(k)(j) = h_res(k)(i)
            }
          }
        }
      }
    }

    //    sig_matrix foreach { row => row.foreach(x => print(x + " ")) ; println }

    val b = 2
    val r = 1
    var cand = Map[(Int, Int), Double]()

    for (i <- 0 until movies_length) {
      for (j <- (i + 1) until movies_length) {
        var flag = false
        var k = 0
        while (k < hash_num && (!flag)) {

          if (!flag) {
            var sig1 = Array.ofDim[Int](r)
            var sig2 = Array.ofDim[Int](r)

            for (m <- 0 until r) {
              sig1(m) = sig_matrix(k * r + m)(i)
              sig2(m) = sig_matrix(k * r + m)(j)

            }

            if (sig1.sameElements(sig2)) {

              cand += ((i, j) -> 0)
              flag = true
            }
          }

          k += r
        }
      }
    }
    //    cand.take(5).foreach(println)
    var real_pairs = ArrayBuffer[(Int, Int, Double)]()

    cand.foreach(x => {
      val p1 = x._1._1
      val p2 = x._1._2
      val jac = jaccard(p1, p2, char_matrix)

      if (jac >= 0.5) {
        //        println(jac)
        val res =  (p1, p2, jac)
        real_pairs += res
      }
    })

    //    real_pairs.sorted.take(5).foreach(println)
    real_pairs = real_pairs.sorted


//    val truth_jaccard = sc.textFile(file_name2)
//
//    var truth_set = Set[(Int, Int)]()
//    truth_jaccard.collect().foreach(x => {
//      val p1 = movies_dict(x.split(',')(0).toInt)
//      val p2 = movies_dict(x.split(',')(1).toInt)
//      truth_set += ((p1, p2))
//    })
//
//    val predict_pairs_num = real_pairs.size
//    val truth_pairs_num = truth_set.size
//    var precision = 0.0
//    var recall = 0.0
//    var TP = 0
//    var FP = 0
//    var FN = 0
//
//
//    real_pairs.foreach(x => {
//      val test = (x._1, x._2)
//      if (truth_set.contains(test)) {
//        TP += 1
//      }
//
//    })
//
//    FP = predict_pairs_num - TP
//    FN = truth_pairs_num - TP
////    val FN_2 = truth_set.count(x => !real_pairs.contains(x))
//
//    println("TP, FP, FN: " + TP + " " + FP + " " + FN)
//    precision = TP.toDouble / (TP + FP).toDouble
//    recall = TP.toDouble / (TP + FN).toDouble
////    val recall_2 = TP.toDouble / (TP + FN_2).toDouble
//
//    println(precision + " " + recall)
////    println("Recall 2", recall_2)



    val file = new File(outFileName)
    val bw = new BufferedWriter(new FileWriter(file))
    real_pairs.foreach(x => {
      val str = x._1.toString + "," + x._2.toString + "," + x._3.toString + "\n"
      bw.write(str)
    })
    bw.close()


    val endtime = System.nanoTime()
    println(endtime - starttime)

  }
}
