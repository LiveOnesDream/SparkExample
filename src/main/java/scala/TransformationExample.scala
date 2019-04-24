package scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TransformationExample {

  val sconf = new SparkConf().setAppName("TransformationExample").setMaster("local[2]")
  val sparkSession = SparkSession.builder().config(sconf).getOrCreate()

  /**
    * 给value对应函数操作
    */
  def mapvalue(): Unit = {
    val list = List("hadoop", "spark", "hive", "spark")
    val rdd = sparkSession.sparkContext.makeRDD(list);
    val pairRdd = rdd.map(x => (x, 1))
    pairRdd.mapValues(_ + 1).collect.foreach(println)
  }
  case class Book(title: String, words: String)
  def flatmap_Example(): Unit ={

    val dfList = List(("Hadoop", "Java,SQL,Hive,HBase,MySQL"), ("Spark", "Scala,SQL,DataSet,MLlib,GraphX"))

    val df=dfList.map{p=>Book(p._1,p._2)}

  }

  def main(args: Array[String]): Unit = {
    mapvalue()
  }

}
