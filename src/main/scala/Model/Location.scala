package Model

import scala.util.Random

final case class Location(latitude : Double, longitude : Double)

object Location {

  val latMinMax = List(48.139278,49.206730)
  val lonMinMax = List(1.478589,3.477713)

  def getParisianLocation() : Location = {
    val rand = new Random()
    val latitude = rand.between(latMinMax(0),latMinMax(1))
    val longitude = rand.between(lonMinMax(0),lonMinMax(1))
    Location(latitude, longitude)
  }

  def updateLocation(location : Location) : Location = {
    val random = new Random()
    val randomSigns = List.fill(2)(Random.nextInt(3) - 1)
    val randomDeltas = List.fill(2)(random.nextInt(10))
    val deltas = randomSigns.zip(randomDeltas).map { case (x, y) => x * y }
    (location.latitude + (deltas(0)/10000000000000000.0) , location.longitude + (deltas(1)/10000000000000000.0)) match {
      case (a,b) if (a > latMinMax(1) || b > lonMinMax(1) || a < latMinMax(0) || b < lonMinMax(0)) => updateLocation(location : Location)
      case (a,b) if (a <= latMinMax(1) && b <= lonMinMax(1) && a >= latMinMax(0) && b >= lonMinMax(0) ) => Location(a , b)
    }
  }
}