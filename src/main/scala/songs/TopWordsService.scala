package songs

import org.apache.spark.rdd.RDD

/**
  * @author Evgeny Borisov
  */
trait TopWordsService {
    def topX(lines:RDD[String],x:Int):List[String]
}
