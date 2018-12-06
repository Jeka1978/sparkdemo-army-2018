package songs
import org.apache.spark.rdd.RDD

/**
  * @author Evgeny Borisov
  */
class TopWordsServiceImpl extends TopWordsService{
  var userConfig:UserConfig = new UserConfig()

  override def topX(lines: RDD[String], x: Int): List[String] = {

    val wordsRdd = lines.map(_.toLowerCase).flatMap("\\w+".r.findAllIn(_))
    wordsRdd.filter(!userConfig.getGarbageWords().contains(_))
      .map((_,1))
      .reduceByKey(_+_)
      .map(_.swap)
      .sortByKey(ascending = false)
      .map(_._2)
      .take(x).toList


  }
}
