import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.tribbloid.ispark.display.dsl._
import scala.util.Try

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

// Declare a CASE class; we need it for the dataframe
case class Row(categoryId: Long, orderId: String ,cityId: String, osName: String,
               osFamily: String, uaType: String, uaName: String,aov: Double)

// read the file into the val variable using sc (Spark Context), it is declared beforehand     
val aov = sc.textFile("file:///Users/rzykov/Downloads/AOVC.csv")

// let's parse the fields
val dataAov = aov.flatMap { line => Try { line.split(",") match {
    case Array(categoryId, orderId, cityId, osName, osFamily, uaType, uaName, aov) =>
        Row(categoryId.toLong + 100, orderId, cityId, osName, osFamily, osFamily, uaType, aov.toDouble)
    } }.toOption }

//OUT:
MapPartitionsRDD[4] at map at <console>:28


//CODE:
val interestedBrowsers = List("Android", "OS X", "iOS", "Linux", "Windows")
val osAov = dataAov.filter(x => interestedBrowsers.contains(x.osFamily)) //we leave only the desired OS
    .filter(_.categoryId == 128) // filter categories
    .map(x => (x.osFamily, (x.aov, 1.0))) // need to calculate average purchase amount
    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    .map{ case(osFamily, (revenue, orders)) => (osFamily, revenue/orders) }
    .collect()
 
//OUT
//The output is an array of tuples (tuple) in OS format, the average purchase amount:
Array(
(OS X,4859.827586206897), 
(Linux,3730.4347826086955), 
(iOS,3964.6153846153848), 
(Android,3670.8474576271187),
(Windows,3261.030993042378))


//CODE of Highcharts

import com.quantifind.charts.Highcharts._
import ru.retailrocket.ispark.wisp._

draw(column(osAov.toList))


//CODE of MIC

import data.VarPairData
import mine.core.MineParameters
import analysis.Analysis
import analysis.results.BriefResult
import scala.util.Random 
 
//Code a discrete value by randomly changing the order of the "codes
def encode(col: Array[String]): Array[Double] = {
 
    val ns = scala.util.Random.shuffle(1 to col.toSet.size)
    val encMap = col.toSet.zip(ns).toMap
    col.map{encMap(_).toDouble}
}
 
// function to calculate MIC
def mic(x: Array[Double], y: Array[Double]) = {
    val data = new VarPairData(x.map(_.toFloat), y.map(_.toFloat))
    val params = new MineParameters(0.6.toFloat, 15, 0, null)
 
    val res = Analysis.getResult(classOf[BriefResult], data, params)
    res.getMIC
}
 
//in case of a discrete value do a lot of iterations and take the maximum
def micMax(x: Array[Double], y: Array[Double], n: Int = 100) = 
    (for{ i <- 1 to 100} yield mic(x, y)).max 
 
//Now we're close to the final result, let's perform the calculation:

val aov = dataAov.filter(x => interestedBrowsers.contains(x.osFamily)) //we leave only the OSes we want
    .filter(_.categoryId == 128) // filter categories
 
//osFamily
var aovMic = aov.map(x => (x.osFamily, x.aov)).collect()
println("osFamily MIC =" + micMax(encode(aovMic.map(_._1)), aovMic.map(_._2))
 
//orderId
 
aovMic = aov.map(x => (x.orderId, x.aov)).collect()
println("orderId MIC =" + micMax(encode(aovMic.map(_._1)), aovMic.map(_._2))
 
//cityId
aovMic = aov.map(x => (x.cityId, x.aov)).collect()
println("cityId MIC =" + micMax(encode(aovMic.map(_._1)), aovMic.map(_._2))
 
//uaName
aovMic = aov.map(x => (x.uaName, x.aov)).collect()
println("uaName MIC =" + mic(encode(aovMic.map(_._1)), aovMic.map(_._2))
 
//aov
println("aov MIC =" + micMax(aovMic.map(_._2), aovMic.map(_._2))
 
//random
println("random MIC =" + mic(aovMic.map(_ => math.random*100.0), aovMic.map(_._2))
 
//OUTPUT
//  osFamily MIC =0.06658
//  orderId MIC =0.10074
//  cityId MIC =0.07281
//  aov MIC =0.99999
//  uaName MIC =0.05297
//  random MIC =0.10599
