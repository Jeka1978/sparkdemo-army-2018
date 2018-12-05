package taxi

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Evgeny Borisov
  */
object TaxiMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("taxi").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rddLines = sc.textFile("data/taxi_order.txt")
    println(rddLines.count())
  }
}
