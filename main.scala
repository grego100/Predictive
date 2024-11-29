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
