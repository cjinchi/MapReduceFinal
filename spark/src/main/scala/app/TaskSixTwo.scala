package app

import org.apache.spark.{SparkConf, SparkContext}

object TaskSixTwo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TaskSixTwo")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("result/task5_out")
    val data = lines.map(stepOne).sortBy(_._1)
    data.saveAsTextFile("result/task6_2_out")
  }

  def stepOne(line: String): ( Int,String) = {
    val temp = line.trim
    val items = temp.substring(1, temp.length - 1).split(",")
    (items(1).toInt,items(0) )
  }

}
