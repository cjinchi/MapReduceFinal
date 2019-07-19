package app

import java.io.{BufferedReader, FileReader}

import app.TaskOne.TAG_NAME
import org.ansj.library.DicLibrary
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object TaskFive {
  val LOOP_TIMES = 40
  var roleNameToId = Map[String, String]()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TaskFive")
    val sc = new SparkContext(conf)
    var index = 0

    val reader = new BufferedReader(new FileReader("data/People_List_unique.txt"))
    var temp: String = reader.readLine()
    while (temp != null) {
      roleNameToId = roleNameToId.updated(temp.trim, index.toString)
      index += 1

      temp = reader.readLine()
    }

    val lines = sc.textFile("result/task3_out")
    var data = lines.flatMap(stepOne).groupByKey()

    for (loop <- 1 to LOOP_TIMES) {
      data = data.flatMap(stepTwo).groupByKey()
    }

    data.map(stepThree).sortBy(_._1).saveAsTextFile("result/task5_out")

  }

  def stepOne(line: String): List[(String, String)] = {
    var ret = List[(String, String)]()
    val temp = line.trim
    val items = temp.substring(1, temp.length - 2).split(",\\[")
    // Type 1: <roleName,#roleId#neighbor1,weight|neighbor2,weight|...>
    ret ::= (items(0), s"#${roleNameToId(items(0))}#${items(1)}")

    val neighbors = items(1).split("\\|")

    for (neighbor <- neighbors) {
      val neighborName = neighbor.trim.split(",")(0)
      // Type 2: <roleName, neighborName#neighborId>
      ret ::= (items(0), neighborName + "#" + roleNameToId(neighborName))
    }
    ret
  }

  def stepTwo(pair: (String, Iterable[String])): List[(String, String)] = {
    var ret = List[(String, String)]()

    var neighborNameToId = Map[String, String]()
    var neighborNameToWeight = Map[String, Double]()
    var idCounter = Map[String, Double]()

    var neighborInfo: String = null
    var roleId: String = null

    for (value <- pair._2) {
      if (value.startsWith("#")) {
        // Type 1: <roleName,#roleId#neighbor1,weight|neighbor2,weight|...>
        val items = value.substring(1).trim.split("#")
        roleId = items(0)
        neighborInfo = items(1)
        for (neighborCommaWeight <- items(1).split("\\|")) {
          val neighborAndWeight = neighborCommaWeight.split(",")
          neighborNameToWeight = neighborNameToWeight.updated(neighborAndWeight(0), neighborAndWeight(1).toDouble)

        }
      } else {
        // Type 2: <roleName, neighborName#neighborId>
        val neighborAndId = value.split("#")
        neighborNameToId = neighborNameToId.updated(neighborAndId(0), neighborAndId(1))

      }
    }

    for (entry <- neighborNameToId.toSet[(String,String)]) {
      val weight = neighborNameToWeight(entry._1)
      val newValue = idCounter.getOrElse(entry._2,0.0)+weight;
      idCounter = idCounter.updated(entry._2, newValue)
    }

    val maxIdsWithCount = idCounter.filter(_._2 >= idCounter.maxBy(_._2)._2)

    if (maxIdsWithCount.get(roleId).isEmpty) {
      val temp = Random.shuffle(maxIdsWithCount.toList)
      roleId = temp.head._1

    }
    ret ::= (pair._1, "#" + roleId + "#" + neighborInfo)
    val roleSharpId = pair._1 + "#" + roleId
    for (neighbor <- neighborNameToId.keySet) {
      ret ::= (neighbor, roleSharpId)
    }
    ret

  }

  def stepThree(pair: (String, Iterable[String])): (String, String) = {
    for (value <- pair._2) {
      if (value.startsWith("#")) {
        // Type 1: <roleName,#roleId#neighbor1,weight|neighbor2,weight|...>
        val items = value.substring(1).trim.split("#")
        return (pair._1, items(0))
      }
    }
    null
  }
}
