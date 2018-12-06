package taxis

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
    val taxiOrderRdd = rddLines
      .map(_.toLowerCase)
      .map(TaxiOrderConverter.convertFromLine)

    println(taxiOrderRdd.filter(_.city == "boston").map(_.km).sum())

    val id2KmRdd = taxiOrderRdd.map(order => (order.id, order.km)).reduceByKey(_ + _)

    val driverRdd = sc.textFile("data/drivers.txt").map(line => {
      val data = line.split(", ")
      Tuple2(data(0).toInt, data(1))
    })
    val top3 = driverRdd.join(id2KmRdd)
      .sortBy(_._2._2,ascending = false).take(3).foreach(println)




  }
}

