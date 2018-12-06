package songs

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Evgeny Borisov
  */
object SongsMain  {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("taxi").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val linesRdd = sc.textFile("data/songs/yest.txt")
    val wordsService = new TopWordsServiceImpl()
    sc.broadcast(new UserConfig)
    println(wordsService.topX(linesRdd,3))

  }
}
