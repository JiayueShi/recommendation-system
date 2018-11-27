import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.math.Ordering.Implicits._
import java.io._

object task {
  def sort[A: Ordering](coll: Seq[Iterable[A]]) = coll.sorted

  def apriori(x: ListBuffer[List[String]], support: Double, total_baskets: Double): Iterator[(List[String], Int)] = {
    //  Begining, I use cur_support = baskets_num / 3, but I ignore each partition may have diff baskets num
    val cur_support = support / (total_baskets / x.size.toDouble)
//    println("threshold" + cur_support )

    var result: ListBuffer[(List[String], Int)] = ListBuffer.empty
    //first map
    var singleMap = HashMap.empty[String, Int]
    val map1 = x.flatten.toList

    for (item <- map1) {
      if (singleMap.contains(item)) {
        singleMap(item) += 1
      } else {
        singleMap += (item -> 1)
      }
    }
    var candidates1 = ListBuffer.empty[String]

    for (item <- singleMap) {
      if (item._2 >= cur_support) {
        candidates1 += item._1
      }
    }
    candidates1 = candidates1.distinct

    // size-1 candidates
    val map1_result = candidates1.map(t => (List(t), 1))

    map1_result.foreach(k => {
      result.append(k)
    })
//    println("size-1 candidate" + result.size)
    //second map
    var map2_data = candidates1.map(t => List(t)).toList
    var itr = 2

    var frequent_num = 1

    while (frequent_num != 0) {
      val candidates = get_comb(x, cur_support, map2_data, itr)
      map2_data = candidates.map(x => x._1)
      candidates.foreach(k => {
        result.append(k)
      })
      frequent_num = map2_data.size
      itr += 1
    }

    result.distinct.toIterator
  }

  def get_comb(x: ListBuffer[List[String]], support: Double, freq_sets: List[List[String]], k: Int): List[(List[String], Int)] = {
    val items = freq_sets.flatten.toList.distinct
//    println("Unique singles:" + items.size)
//    println("cur_k:", k)
    var comb_set = Set.empty[Set[String]]
    for (itemset <- freq_sets) {
      for (item <- items) {
        var newItemset = itemset.toSet
        newItemset += item
        if (newItemset.size == k) {
          comb_set += newItemset
        }
      }
    }
//    println("Possiable sets:" + comb_set.size)
    val counter = comb_set.map(elem => {
      var count = 0
      x.foreach(g => {
        if (elem.forall(g.contains)) {
          count += 1
        }
      })
      (elem.toList.sorted, count)
    })
    var candidates2 = counter.filter(x => x._2.toDouble > support)
    //    var map2_keys = candidates2.map(t => t._1)
//    println("candidates:" + candidates2.size)

    candidates2.toList
  }

  def second_map(x: ListBuffer[List[String]], cand: Array[List[String]]): Iterator[(List[String], Int)] = {

    val counter = cand.map(elem => {
      var count = 0
      x.foreach(g => {
        if (elem.forall(g.contains)) {
          count += 1
        }
      })
      (elem.sorted, count)
    })

    counter.toIterator
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

    // beauty books small2
//    val file_name = "beauty.csv"
    val file_name = args(1)
    val small1 = sc.textFile(file_name).map(line => line.split(","))
    val content = small1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    var baskets: RDD[(String, List[String])] = sc.emptyRDD
//    val support = 40
//    val c: String = "2"
    val c: String = args(0)
    val support = args(2).toInt

    if (c == "1") {
      baskets = content
        .map(x => (x(0), List(x(1))))
        .groupByKey()
        .flatMapValues(res => res.toList)
        .reduceByKey((a, b) => a ++ b)
        .distinct()
    } else if (c == "2") {
      baskets = content
        .map(x => (x(1), List(x(0))))
        .groupByKey()
        .flatMapValues(res => res.toList)
        .reduceByKey((a, b) => a ++ b)
        .distinct()
    } else {
      print("Wrong case number")
      sys.exit()
    }

    val basket_num = baskets.count().toDouble
    //  start map1 of SON
    val map1 = baskets.mapPartitions(x => {

      var listOfLists = ListBuffer.empty[(List[String])]
      x.toList.map(p => listOfLists += p._2)
      apriori(listOfLists, support.toDouble, basket_num.toDouble)
    }).reduceByKey((a, b) => 1)

    val map1_result = map1.keys.collect()

    //  start map2 of SON
    val map2 = baskets.mapPartitions(x => {
      var listOfLists = ListBuffer.empty[(List[String])]
      x.toList.map(p => listOfLists += p._2)
      second_map(listOfLists, map1_result)
    }).reduceByKey((a, b) => a + b).collect()

    val map2_result = map2.filter(m => m._2.toDouble >= support).map(x => x._1.sorted).toList.sortBy(elem => (elem.size, elem))
    val endtime = System.nanoTime()

//    println("Count", map2_result.size)
//    println(endtime - starttime)
    //
    var outFileName = ""
    if (!file_name.equals("small2.csv")){
      outFileName = "Jiayue_Shi_SON_%s.case%s-%s.txt".format(file_name.stripSuffix(".csv").capitalize, c, support)
    }else{
      outFileName = "Jiayue_Shi_SON_%s.case%s.txt".format(file_name.stripSuffix(".csv").capitalize, c)
    }

    val file = new File(outFileName)
    val bw = new BufferedWriter(new FileWriter(file))

    var s = 0
    var i = 0
    while (i < map2_result.length){

      if (map2_result(i).lengthCompare(s) == 0) {
        bw.write("," + "(")
        var j = 0
        while (j < map2_result(i).length) {
          if (j != (map2_result(i).length - 1)) {
            bw.write("'" + map2_result(i)(j) + "'" + ",")
          } else {
            bw.write("'" + map2_result(i)(j) + "'")
          }
          j += 1
        }
        bw.write(")")
      }
      else {
        if (i != 0) {
          bw.write("\n")
          bw.write("\n")
        }

        s = map2_result(i).length
        bw.write("(")
        var j = 0
        while (j < map2_result(i).length) {
          if (j != (map2_result(i).length - 1)) {
            bw.write("'" + map2_result(i)(j) + "'" + ",")
          } else {
            bw.write("'" + map2_result(i)(j) + "'")
          }
          j += 1
        }
        bw.write(")")
      }
    i += 1

  }


    bw.close()


  }
}
