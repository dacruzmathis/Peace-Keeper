package Model

import Model.Words._
import Model._
import Service.Storage

import scala.util.{Random, Try}

final case class Person(id: String, name: String, age : Int, sex : String, club : String, words : List[String], score: Int, timesWithoutInjuries : Int)

object Person {

  def fromCsvLine(line : Array[String]) : Option[Person] = {
    line.size match {
      case 2 => parsePerson(line)
    }
  }

  def parsePerson(line : Array[String]) = {
    (Try(line(0)).toOption, Try(line(1)).toOption) match {
      case (Some(id), Some(name)) => {
        val age = getAge()
        val sex = getSex()
        val club = getClub()
        val words = getWords(age, sex, club)
        Some(Person(id, name, age, sex, club, words, getScore(7, words), 0))
      }
      case _ => None
    }
  }

  def getPersons(persons: Storage[Person]) = {
    persons.storage
      .map(person => "Person(" + person.id + "," + person.name + "," + person.words + "," + person.score + ")")
  }

  def printPersons(persons: Storage[Person]) = {
    persons.storage
      .map(person => "Person(" + person.id + "," + person.name + "," + person.words + "," + person.score + ")")
  }

  def getRandomParisian(persons: List[Person])= {
    val rand = new Random()
    val parisians = persons
    Try(parisians(rand.between(0,parisians.length-1))).getOrElse(parisians(0))
  }

  def dropParisian(person : Person, persons: List[Person])= {
    persons
      .filter(x => x.id != person.id)
  }

  def getParisians(personsWatched:List[Person], persons:List[Person], n:Int) : List[Person] = n match {
    case 0 => personsWatched
    case _ => val parisian = getRandomParisian(persons)
      getParisians(personsWatched:+parisian, dropParisian(parisian, persons), n-1)
  }

  def getAge(): Int = {
    val rand = new Random()
    rand.between(18, 81)
  }

  def getSex(): String = {
    val rand = new Random()
    if (rand.between(0, 2) == 0) "M"
    else "F"
  }

  def getClub(): String = {
    val hasClub = new Random().between(0, 4)
    if (hasClub == 0) "None"
    else {
      val getClub = new Random().between(0, 5)
      List("Real Madrid","FC Barcelona","Paris-Saint-Germain","Bayern Munich","Manchester United")(getClub)
    }
  }

  def getScore(scorePerson : Int , words: List[String]): Int = {
    words match {
      case Nil => scorePerson
      case t :: q => if (getInjuries().contains(t) && scorePerson > 0) getScore(scorePerson - 1, q) else getScore(scorePerson, q)
    }
  }

  def updatePerson(person: Person): Person = {
    val words = getWords(person.age, person.sex, person.club)
    addScoreBonus(Person(person.id, person.name, person.age, person.sex, person.club, words, getScore(person.score , words), setTimesWithoutInjuries(person.timesWithoutInjuries , words)), 2)
  }

  def removeElementByIndex(list: List[Person], index: Int): List[Person] = {
    val (first, second) = list.splitAt(index)
    first ::: second.tail
  }

  def transferNElement(from: List[Person], to: List[Person], nTimes : Int): (List[Person], List[Person]) = nTimes match {
    case 0 => (from, to)
    case _ =>
      val index = new Random().between(0, from.length)
      transferNElement(removeElementByIndex(from, index), to :+ from(index), nTimes-1)
  }

  def addPersonsToZone(peaceWatcher: PeaceWatcher): PeaceWatcher ={
    val numberOfPersonsToAdd = new Random().between(1, peaceWatcher.personsNotWatched.length+1)
    val personsTotal = transferNElement(peaceWatcher.personsNotWatched, peaceWatcher.persons, numberOfPersonsToAdd)
    PeaceWatcher(peaceWatcher.id, personsTotal._2, personsTotal._1, peaceWatcher.location, peaceWatcher.time)
  }

  def dropPersonsFromZone(peaceWatcher: PeaceWatcher): PeaceWatcher ={
    val personsNotWatched = peaceWatcher.personsNotWatched.length
    val numberOfPersonsToDrop = new Random().between(1, if (personsNotWatched == 0) 2 else personsNotWatched + 1)
    val personsTotal = transferNElement(peaceWatcher.persons, peaceWatcher.personsNotWatched, numberOfPersonsToDrop)
    PeaceWatcher(peaceWatcher.id, personsTotal._1, personsTotal._2, peaceWatcher.location, peaceWatcher.time)
  }

  def updatePersonsInTheZone(peaceWatcher: PeaceWatcher): PeaceWatcher = {
      val probabilityToUpdateTheZone = new Random().between(0, 100)
      if(probabilityToUpdateTheZone < 20){
        if(List(5,6,7).contains(peaceWatcher.persons.length)) {
          addPersonsToZone(peaceWatcher)
        }
        else dropPersonsFromZone(peaceWatcher)
      }
      else peaceWatcher
  }

  def addScoreBonus(person : Person, cooldownDuration : Int) : Person = {
    if(person.timesWithoutInjuries == cooldownDuration && person.score != 10) {
        Person(person.id, person.name, person.age, person.sex, person.club, person.words, person.score + 1, 0)
    }
    else person
  }

  def setTimesWithoutInjuries(timesWithoutInjuries : Int, words : List[String]) : Int = {
    words match {
      case Nil => 1 + timesWithoutInjuries
      case t :: q => if (getInjuries().contains(t)) 0 else setTimesWithoutInjuries(timesWithoutInjuries , q)
    }
  }
}
