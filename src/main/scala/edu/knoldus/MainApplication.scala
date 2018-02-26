package edu.knoldus

import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
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
  val customerData = customerfile.map(x =>Customer(x(0).toInt, x(1), x(2), x(3), x(4),x(5).toInt)).toDF()
  val salesData = salesFile.map(x => Sales(Time.getYear(x(0).toInt), Time.getMonth(x(0).toInt), Time.getDay(x(0).toInt), x(1).toInt, x(2).toDouble))


  val innerJoin = salesData.join(customerData, salesData("customer_id") === customerData("customer_id"), "inner")
  innerJoin.select("state", "year", "price").show
  sc.stop()


}
