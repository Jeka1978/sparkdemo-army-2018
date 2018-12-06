package songs

/**
  * @author Evgeny Borisov
  */
class UserConfig extends Serializable {
 def getGarbageWords():List[String]={
   List("i","I","to")
 }
}
