package Model

import Model.Location._
import Model.Person._
import Service.Storing

import java.time.LocalDateTime
import scala.util.Random

final case class PeaceWatcher(id : Int, persons : List[Person], personsNotWatched : List[Person], location : Location, time: LocalDateTime)

object PeaceWatcher {

  val persons : List[Person] = Storing.storingPersons()

  def getOneInstanceOfPeaceWatcher(pers : List[Person] = persons, fixed_date : LocalDateTime): PeaceWatcher = {
    val rand = new Random()
    PeaceWatcher(rand.between(0,9999), getParisians(List(), pers, 10), List(), getParisianLocation(), fixed_date)
  }

  def getInitialsPeaceWatchers : List[PeaceWatcher] = {
    val fixedDate = LocalDateTime.now()
    (1 to 10)
      .toList
      .foldLeft((List.empty[PeaceWatcher], persons)) { case (acc, _) =>
        val peaceData = getOneInstanceOfPeaceWatcher(acc._2, fixedDate)
        val filterPers = acc._2.filterNot(peaceData.persons.contains)
        val accPeace = peaceData :: acc._1
        (accPeace, filterPers)
      }._1
  }

  def updatePeaceWatcher(peaceWatcher : PeaceWatcher, fixedDate : LocalDateTime) : PeaceWatcher = {
    updatePersonsInTheZone(PeaceWatcher(peaceWatcher.id , peaceWatcher.persons.map(p => updatePerson(p)), peaceWatcher.personsNotWatched, updateLocation(peaceWatcher.location) , fixedDate))
  }

  def makeIterations(listOfPw: List[PeaceWatcher]): LazyList[List[PeaceWatcher]] = {

    def iterateWithFixedDate(list: List[PeaceWatcher], fixedDate: LocalDateTime): List[PeaceWatcher] = {
      list.map(peace => updatePeaceWatcher(peace, fixedDate))
    }

    LazyList.iterate(listOfPw) { list =>
      val fixedDate = LocalDateTime.now()
      iterateWithFixedDate(list, fixedDate)
    }
  }
}