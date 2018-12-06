package taxis

/**
  * @author Evgeny Borisov
  */
object TaxiOrderConverter {
  def convertFromLine(line: String): TaxiOrder = {
    val data = line.split(" ")
    TaxiOrder(id = data(0).toInt,city = data(1),km = data(2).toInt)
  }
}
