package linkedIn

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
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

   /* val session = SparkSession.builder().appName("").master("").getOrCreate()
    val frame: DataFrame = session.read.csv("")
*/

    val conf = new SparkConf().setAppName("linked in").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read.json("data/profiles.json")
    dataFrame.show()
    dataFrame.printSchema()
    dataFrame.schema.fields.foreach(field => println(field.name))
    val withSalaryDF = dataFrame.withColumn("salary", col("age") * 10 * size(col("keywords")))
    withSalaryDF.persist(StorageLevel.MEMORY_AND_DISK).show()
    val keywordDf = dataFrame.withColumn("keyword",explode(col("keywords"))).select("keyword")
    val sortedKeywordsDF = keywordDf.groupBy(col("keyword")).agg(count(col("keyword")).as("amount"))
      .orderBy(desc("amount"))
    sortedKeywordsDF.show()
    val row = sortedKeywordsDF.head()
    val mostPopular:String
    = row.getAs("keyword")
    withSalaryDF.filter(array_contains(col("keywords"),mostPopular))
      .where(col("salary")<1200).show()
   Thread.sleep(Integer.MAX_VALUE)
//    withSalaryDF.withColumn("NIS salary")
//      .where("salary<1200")
//      .where(col("salary").leq(1200))


  }
}


