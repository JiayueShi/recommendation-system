import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}
import scala.math.Ordering.Implicits._
import java.io._

import org.apache.spark.graphx._

import scala.collection.mutable

object Betweenness {
  var result:Array[((Int, Int), Double)] = Array[((Int, Int), Double)]()
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

    val file_name1 = args(0)
    val outputpath = args(1)

//    val file_name1 = "data/video_small_num.csv"
//    val outputpath = "data/"
    var data = sc.textFile(file_name1)
    val data_header = data.first()
    data = data.filter(x => x != data_header)

    val user_and_movies = data.map(_.split(',') match { case Array(u, m, r, t) => (u.toInt,Set(m.toInt))})
      .reduceByKey(_++_).cache()
    val edges = user_and_movies.cartesian(user_and_movies).filter(x => {
      x._1._1 != x._2._1 && x._1._2.intersect(x._2._2).size >= 7
    }).map(x => {(x._1._1, Set(x._2._1))})

    val graph = edges.reduceByKey(_ ++ _)

    val neig = graph.collectAsMap()
    val vertices = graph.keys


    val raw_b = vertices.map(x => {
      var betweenness = mutable.Map[(Int, Int), Double]().withDefaultValue(0.0)

      var q = mutable.Queue[(Int, Int)]()
      val level_nodes = mutable.HashMap[Int, Set[Int]]().withDefaultValue(Set[Int]())
      val visited = mutable.Set[Int]()

      val root = x
      q.enqueue((root, 1))
      visited += root

      val shortest_map = mutable.Map[Int, Int]().withDefaultValue(0)
      shortest_map(root) = 1

      while(q.nonEmpty){
        val cur_v = q.dequeue()
        val node = cur_v._1
        val level = cur_v._2


        level_nodes(level) += node

        if (neig.contains(node)){
          val children = neig(node)
          children.foreach(x => {
            if(!visited.contains(x)){
              q.enqueue((x, level + 1))
              visited += x
            }else{
              if(level_nodes(level - 1).contains(x)){
                shortest_map(node) = shortest_map(node) + shortest_map(x)
              }
            }
          })
        }
        }

      var depth = level_nodes.size

      val weight_map = new mutable.HashMap[Int, Double]().withDefaultValue(0.0)

      while (depth > 1){
        for (cur_node <- level_nodes(depth)) {
          var parents = ListBuffer[Int]()
          val cur_neig = neig.getOrElse(cur_node, Set())


          for (upper_node <- level_nodes(depth - 1)) {
            if(cur_neig.contains(upper_node)){
              parents += upper_node
            }
          }
//          println(parents.length)

          val cur_weight = weight_map.getOrElse(cur_node, 1.0)
//          val seperate_weight = cur_weight  / parents.length.toDouble
          var seperate_weight = 0.0
          if(parents.length <= 1){
            seperate_weight = cur_weight
            parents.foreach(p =>{

              val parent_weight = weight_map.getOrElse(p, 1.0)

              weight_map(p) = parent_weight + seperate_weight

              if(cur_node < p){
                betweenness((cur_node, p)) = betweenness.getOrElse((cur_node, p), 0.0) + seperate_weight
              }
              else{
                betweenness((p, cur_node)) = betweenness.getOrElse((p, cur_node), 0.0) + seperate_weight
              }
            })
          }
          else{

            parents.foreach(p =>{
//              println(shortest_map(cur_node) + " " + shortest_map(p))
              seperate_weight = cur_weight * shortest_map(p).toDouble / shortest_map(cur_node).toDouble

              val parent_weight = weight_map.getOrElse(p, 1.0)

              weight_map(p) = parent_weight + seperate_weight

              if(cur_node < p){
                betweenness((cur_node, p)) = betweenness.getOrElse((cur_node, p), 0.0) + seperate_weight
              }
              else{
                betweenness((p, cur_node)) = betweenness.getOrElse((p, cur_node), 0.0) + seperate_weight
              }
            })


          }
        }
        depth -= 1

      }
      betweenness

    })


    val total_b = raw_b.flatMap(x => x).reduceByKey((w1,w2)=>w1+w2).map(x => (x._1, x._2 / 2.0)).collect().sorted

    val writer = new PrintWriter(new File(outputpath + "Jiayue_Shi_Betweenness.txt"))
    total_b.foreach(x =>
        writer.write("(" + x._1._1 + "," + x._1._2 + "," + x._2 + ")\n" )
      )
    writer.close()





    val endtime = System.nanoTime()
    val total_time = (endtime - starttime) / 1000000000
//    println(total_time)

  }

}
