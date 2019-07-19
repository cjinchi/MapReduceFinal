package app

import org.apache.spark.{SparkConf, SparkContext}

object TaskThree {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TaskTwo")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("result/task2_out")
    val result = lines.map(parseLine)
      .reduceByKey(_ ++ _)
      .map(normalize)
      .sortBy(_._1)
    result.saveAsTextFile("result/task3_out")


  }

  def parseLine(line: String): (String, List[(String, Int)]) = {
    var str = line.trim
    var items = str.substring(1, str.length - 1).split(">,")
    val num: Int = items(1).toInt
    var names = items(0).substring(1).split(",")
    (names(0), List((names(1), num)))
  }

  def normalize(pairs: (String, List[(String, Int)])): (String, String) = {
    var total = 0
    pairs._2.foreach(total += _._2)
    var neighbors = List[String]()
    for (pair <- pairs._2) {
      val name = pair._1
      val weight = pair._2.toFloat / total
      neighbors ::= s"$name,$weight"
    }
    (pairs._1, "[" + neighbors.mkString("|") + "]")
  }

}
