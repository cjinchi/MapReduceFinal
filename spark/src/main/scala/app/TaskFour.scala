package app

import org.apache.spark.{SparkConf, SparkContext}

object TaskFour {
  val LOOP_TIMES = 25
  val PR_INIT = 1.0
  val DAMPING = 0.85

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TaskFour")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("result/task3_out")
    var data = lines.map(stepOne)
    var loop = 1
    for (loop <- 1 to LOOP_TIMES) {
      data = data.flatMap(stepTwo).groupByKey.map(stepThree)
    }
    val result = data.map(stepFour)
    result.saveAsTextFile("result/task4_out")
  }

  def stepOne(line: String): (String, String) = {
    val string = line.trim
    val items = string.substring(1, string.length - 2).split(",\\[")
    (items(0), s"$PR_INIT#" + items(1))

  }

  def stepTwo(pair: (String, String)): List[(String, String)] = {
    var ret = List[(String, String)]()
    val items = pair._2.trim.split("#")
    ret ::= (pair._1, items(1))
    val neighbors = items(1).trim.split("\\|")
    val currentPr = items(0).toDouble
    for (neighbor <- neighbors) {
      val neighborAndWeight = neighbor.split(",")
      ret ::= (neighborAndWeight(0), "#" + (currentPr * neighborAndWeight(1).toDouble).toString)
    }
    ret
  }

  def stepThree(pair: (String, Iterable[String])): (String, String) = {
    var sum: Double = 0
    var neighbors: String = null
    for (value <- pair._2) {
      if (value.startsWith("#")) {
        sum += value.substring(1).toDouble
      } else {
        neighbors = value
      }
    }
    var newPr = 1.0 - DAMPING + DAMPING * sum
    (pair._1, s"$newPr#$neighbors")
  }

  def stepFour(pair: (String, String)): (String, Double) = {
    val pr = pair._2.split("#")(0).toDouble
    (pair._1, pr)
  }
}
