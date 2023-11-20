package Service

import fileReader.service.CSV
import Model._

import scala.io.Source

final case class Storage[A](storage : List[A])

object Storing {

  def storingPersons[A](): List[Person] = {
    CSV.read("src/resources/family-name.csv", Person.fromCsvLine).lines
  }

  def storingWords[A](): List[String] = {
    val file = Source.fromFile("src/resources/words&injuries")
    file.getLines().flatMap(x => x.split(",").map(_.trim)).toList
  }
}