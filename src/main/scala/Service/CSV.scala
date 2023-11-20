package fileReader.service

import scala.io.Source

object CSV {
  final case class ReadResult[A](lines : List[A])

  def read[A](fileName : String , parseLine : Array[String] => Option[A] , regex : String = ";") = {
    val file = Source.fromFile(fileName)
    val scalaFileContents : Iterator[String] = file.getLines()
    val lineParsed = scalaFileContents.map(data => parseLine(data.split(regex).map(_.trim))).toList
    ReadResult(lineParsed.flatten)
  }
}
