package app

import org.apache.spark.{SparkConf, SparkContext}

object TaskTwo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TaskTwo")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("result/task1_out")
    val tuples = lines.flatMap(lineToTuple).reduceByKey(_ + _).sortBy(_._1).repartition(1)
    tuples.saveAsTextFile("result/task2_out")


  }

  def lineToTuple(line: String): List[(String,Int)] = {
    var ret = List[(String,Int)]()
    val roleNames = line.trim.split(" ").toSet
    for (a <- roleNames) {
      for (b <- roleNames) {
        if (!a.equals(b)) {
          ret ::= (s"<$a,$b>",1)
        }
      }
    }
    ret
  }
}
