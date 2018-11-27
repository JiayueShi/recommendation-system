package com.soundcloud.lsh

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]) {
    val numPartitions = 8

    val conf = new SparkConf()
      .setAppName("LSH-Cosine")
      .setMaster("local[*" +
        "]")
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    val sc = new SparkContext(conf)

    val file_name1 = "data/video_small_num.csv"
    var data = sc.textFile(file_name1)
    val data_header = data.first()
    data = data.filter(x => x != data_header)

    val pairs = data.map(line => line.split(",")(0) + " " + line.split(",")(1))
    val users = data.map(line => line.split(",")(0).toInt).distinct().collect().sorted
    val movies = data.map(line => line.split(",")(1).toInt).distinct().collect().sorted

    val users_length = users.length
    val movies_length = movies.length

    var users_dict = Map[Int, Int]()
    var movies_dict = Map[Int, Int]()

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

    val char_matrix = Array.ofDim[Double](movies_length, users_length)
//    val char_matrix = Array.ofDim[Double](users_length, movies_length)

    pairs.collect().foreach(x => {
      val user_id = x.split(" ")(0).toInt
      val movie_id = x.split(" ")(1).toInt
//      char_matrix(users_dict(user_id))(movies_dict(movie_id)) = 1
      char_matrix(movies_dict(movie_id))(users_dict(user_id)) = 1.0
    })

    val m = char_matrix.zipWithIndex.map(x => {

      (x._2.toString, x._1)
    })

    // Convert back to rdd to fit system
    val dataRDD = sc.parallelize(m, numPartitions)

    // create an unique id for each word by zipping with the RDD index
    val indexed = dataRDD.zipWithIndex.persist(storageLevel)

    // create indexed row matrix where every row represents one word
    val rows = indexed.map {
      case ((word, features), index) =>
        IndexedRow(index, Vectors.dense(features))
    }

    // store index for later re-mapping (index to word)
    val index = indexed.map {
      case ((word, features), index) =>
        (index, word)
    }.persist(storageLevel)

    // create an input matrix from all rows and run lsh on it
    val matrix = new IndexedRowMatrix(rows)
    val lsh = new Lsh(
      minCosineSimilarity = 0.5,
      dimensions = 50,
      numNeighbours = 200,
      numPermutations = 9,
      partitions = numPartitions,
      storageLevel = storageLevel
    )
    val similarityMatrix = lsh.join(matrix)

    // remap both ids back to words
    val remapFirst = similarityMatrix.entries.keyBy(_.i).join(index).values
    val remapSecond = remapFirst.keyBy { case (entry, word1) => entry.j }.join(index).values.map {
      case ((entry, word1), word2) =>
        (word1.toInt, word2.toInt, entry.value)
    }

//    var res_set = Set[(Int, Int)]()

    val res = remapSecond.collect().sorted
//    res.foreach(x => {
//      res_set += ((x._1, x._2))
//    })


    val file_name2 = "data/video_small_ground_truth_cosine.csv"
    val truth_jaccard = sc.textFile(file_name2)

    var truth_set = Set[(Int, Int)]()
    truth_jaccard.collect().foreach(x => {
      val p1 = movies_dict(x.split(',')(0).toInt)
      val p2 = movies_dict(x.split(',')(1).toInt)
      truth_set += ((p1, p2))
    })

    val predict_pairs_num = res.length
    val truth_pairs_num = truth_set.size
    var precision = 0.0
    var recall = 0.0
    var TP = 0
    var FP = 0
    var FN = 0


    res.foreach(x => {
      val test = (x._1.toInt, x._2.toInt)
      if (truth_set.contains(test)) {
        TP += 1
      }

    })

    FP = predict_pairs_num - TP
    FN = truth_pairs_num - TP
//    val FN_2 = truth_set.count(x => !res_set.contains(x))

    println("TP, FP, FN: " + TP + " " + FP + " " + FN)
    precision = TP.toDouble / (TP + FP).toDouble
    recall = TP.toDouble / (TP + FN).toDouble
//    val recall_2 = TP.toDouble / (TP + FN_2).toDouble

    println(precision + " " + recall)
//    println("Recall 2", recall_2)

    var outFileName = "data/Jiayue_Shi_SimilarProducts_Cosine.txt"
    val file = new File(outFileName)
    val bw = new BufferedWriter(new FileWriter(file))
    res.foreach(x => {
      val str = x._1.toString + "," + x._2.toString + "," + x._3.toString + "\n"
      bw.write(str)
    })
    bw.close()


  }
}