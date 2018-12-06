package linkedIn

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Evgeny Borisov
  */

// Print out the schema
//Print out the column type
//Add column “salary” the value will be calculated by formula:age * number of technologies * 10
//Find developers with salary < 1200 and which familiar with most popular technology

object ScalaMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("linked in").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.json("data/profiles.json")
    dataFrame.show()


  }
}
