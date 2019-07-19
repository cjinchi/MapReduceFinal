package app

import org.apache.spark.{SparkConf, SparkContext}

object TaskSixOne {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TaskSixOne")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("result/task4_out")
    val data = lines.map(stepOne).sortBy(_._2,ascending = false)
    data.saveAsTextFile("result/task6_1_out")
  }

  def stepOne(line:String):(String,Double)={
    val temp = line.trim
    val items = temp.substring(1,temp.length-1).split(",")
    (items(0),items(1).toDouble)
  }
}
