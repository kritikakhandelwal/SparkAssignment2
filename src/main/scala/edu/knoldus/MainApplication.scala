package edu.knoldus

import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Time {

  def getYear(time: Long): Int = {
    val date = new Date(time * 1000L)
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.YEAR)
  }

  def getMonth(time: Long): Int = {
    val date = new Date(time * 1000L)
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.MONTH) + 1
  }

  def getDay(time: Long): Int = {
    val date = new Date(time * 1000L)
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.DAY_OF_MONTH)
  }
}

object Application extends App {


  Logger.getLogger("org").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("SparkAssignment").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val customerFile1 = sc.textFile("/home/knoldus/Desktop/File1.txt")
  val salesFile1 = sc.textFile("/home/knoldus/Desktop/File2.txt")
  val customerfile = customerFile1.map(line => line.split("#"))
  val salesFile = salesFile1.map(line => line.split('#'))
  val customerData = customerfile.map(x =>(x(0).toInt,Customer(x(0).toInt, x(1), x(2), x(3), x(4),x(5).toInt)))
  val salesData = salesFile.map(x =>(x(1).toInt,Sales(Time.getYear(x(0).toInt), Time.getMonth(x(0).toInt), Time.getDay(x(0).toInt), x(1).toInt, x(2).toDouble)))

  val innerJoin = customerData join salesData

  val yearlyRecord = innerJoin.map(x => ((x._2._1.state, x._2._2.year), x._2._2.price))
    .reduceByKey(_ + _).map(y => s"${y._1._1}#${y._1._2}###${y._2}")

  val monthlyRecord = innerJoin.map(x => ((x._2._1.state, x._2._2.year, x._2._2.month), x._2._2.price))
    .reduceByKey(_ + _).map(y => s"${y._1._1}#${y._1._2}#${y._1._3}##${y._2}")

  val dailyRecord = innerJoin.map(x => ((x._2._1.state, x._2._2.year, x._2._2.month, x._2._2.day), x._2._2.price))
    .reduceByKey(_ + _).map(y => s"${y._1._1}#${y._1._2}#${y._1._3}#${y._1._4}#${y._2}")

  val record = yearlyRecord union  monthlyRecord union  dailyRecord

  record.repartition(1);

  record.saveAsTextFile("/home/knoldus/Desktop/result.txt");

  println("\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n")
  println(s"${yearlyRecord.collect.toList}\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
  println(s"${monthlyRecord.collect.toList}\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
  println(s"${dailyRecord.collect.toList}\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")

  sc.stop()


}
