import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}
import scala.math.Ordering.Implicits._
import java.io._

import org.apache.spark.graphx._

import scala.collection.mutable


object Community {
  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }
  var final_comm = mutable.Map[Int, mutable.Set[Int]]()

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

          val cur_weight = weight_map.getOrElse(cur_node, 1.0)
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
    val sorted_b = total_b.sortBy(_._2).reverse

    val v_num = vertices.collect().length
    val m = total_b.length
    val degree_map = mutable.Map[Int, Int]().withDefaultValue(0)

    val edge_set = mutable.Set[(Int, Int)]()
    sorted_b.foreach(x =>{

      val pair = (x._1._1, x._1._2)
      degree_map(x._1._1) += 1
      degree_map(x._1._2) += 1
      edge_set += pair
    })
    val vertice_set = vertices.collect().toSet

    var index = -1

    var cur_graph = collection.mutable.Map(neig.toSeq: _*)
    var final_graph = collection.mutable.Map(neig.toSeq: _*)

    var max_m = -2.0

    for (i <- 0 until m) {
      val del = sorted_b(i)
      val del_v1 = del._1._1
      val del_v2 = del._1._2

      var set1 = cur_graph(del_v1)
      cur_graph(del_v1) = set1.filter(_!=del_v2)

      var set2 = cur_graph(del_v2)
      cur_graph(del_v2) = set2.filter(_!=del_v1)

      degree_map(del_v1) -= 1
      degree_map(del_v2) -= 1

      var cur_m = modularity(cur_graph, degree_map, edge_set, m, vertice_set)
//      println("cur_m" + " " + index + " " + cur_m)
      if(cur_m > max_m){
        max_m = cur_m
        index = i
      }
    }

//    println("max_m" + " " + index + " " + max_m)

    for (i <- 0 until (index + 1)) {
      val del = sorted_b(i)
      val del_v1 = del._1._1
      val del_v2 = del._1._2

      var set1 = final_graph(del_v1)
      final_graph(del_v1) = set1.filter(_!=del_v2)

      var set2 = final_graph(del_v2)
      final_graph(del_v2) = set2.filter(_!=del_v1)
    }

    val raw_res = get_comm(final_graph, vertice_set)
    val res = raw_res.map(x => {
      x._2.toList.sorted
    }).toList.sorted

    val writer = new PrintWriter(new File(outputpath + "Jiayue_Shi_Community.txt"))
    res.foreach(x => {
      writer.write("[")
      writer.write(x.head + "")

      x.tail.foreach(w => {
        writer.write("," + w)
      })
      writer.write("]\n")

    })
    writer.close()

    val endtime = System.nanoTime()
    val total_time = (endtime - starttime) / 1000000000
    println(total_time)




  }

  def modularity(cur_graph: mutable.Map[Int, Set[Int]], degree: mutable.Map[Int, Int], edge_set: mutable.Set[(Int, Int)], m: Int, vertice_set: Set[Int]):Double = {
    var modu = 0.0

    var cur_comm = get_comm(cur_graph, vertice_set)

    modu = cur_comm.map(c => {
      val pairs = c._2.cross(c._2).toList.filter(elem => elem._1 < elem._2)
      var s = 0.0
      pairs.foreach(a => {
        val v1 = a._1
        val v2 = a._2
        var A = 0.0
        if(edge_set.contains(a)){
          A = 1.0
        }
        var k1 = degree.getOrElse(v1, 0).toDouble
        var k2 = degree.getOrElse(v2, 0).toDouble

        s += A - k1 * k2 / (2 * m).toDouble
      })

      s
    }).sum / (2 * m).toDouble

    modu
  }

  def get_comm(cur_graph: mutable.Map[Int, Set[Int]], vertice_set: Set[Int]): mutable.Map[Int, mutable.Set[Int]] ={
    var cur_comm = mutable.Map[Int, mutable.Set[Int]]()

    var visited = mutable.Set[Int]()
    var remain_vertice = vertice_set


    var cur_comm_num = 0

    while(remain_vertice.nonEmpty){
      var root = remain_vertice.head
      var comm_nodes = mutable.Set[Int]()

      comm_nodes = bfs(cur_graph, root)

      visited = visited.union(comm_nodes)
      cur_comm += (cur_comm_num -> comm_nodes)
      remain_vertice = vertice_set.diff(visited)

      cur_comm_num += 1
//      comm_nodes.clear()
    }

    cur_comm
  }

  def bfs(cur_graph: mutable.Map[Int, Set[Int]], root: Int): mutable.Set[Int] = {
    var q = mutable.Queue[Int]()
    var visited = mutable.Set[Int]()

    q.enqueue(root)
    visited += root

    while(q.nonEmpty){
      val cur_v = q.dequeue()
      val node = cur_v

      if (cur_graph.contains(node)){
        val children = cur_graph(node)
        children.foreach(x => {
          if(!visited.contains(x)){
            q.enqueue(x)
            visited += x
          }
        })
      }
    }

    visited
  }



}
