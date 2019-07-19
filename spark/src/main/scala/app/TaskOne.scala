package app

import java.io.{BufferedReader, FileReader}

import org.ansj.domain.Term
import org.ansj.library.DicLibrary
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

object TaskOne {
  val TAG_NAME = "jyxsrw"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TaskOne")
    val sc = new SparkContext(conf)
    val reader = new BufferedReader(new FileReader("data/People_List_unique.txt"))
    var temp: String = reader.readLine()
    while (temp != null) {
      DicLibrary.insert(DicLibrary.DEFAULT, temp.trim, TAG_NAME, Int.MaxValue)
      temp = reader.readLine()
    }
    val novels = sc.textFile("data/novels")
    val roleNames = novels.map(getRoleNames).filter(_.trim.length>0)
    roleNames.saveAsTextFile("result/task1_out")
  }
//
  def getRoleNames(line: String): String = {
    var terms =  DicAnalysis.parse(line).getTerms
    val roleNames = terms.asScala.filter(_.getNatureStr.equals(TAG_NAME)).map(term => term.getName).mkString(" ")
    roleNames
  }
}
