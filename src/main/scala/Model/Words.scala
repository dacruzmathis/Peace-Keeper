package Model

import Model.Words.getRandomWords
import Service.{Storage, Storing}

import scala.::
import scala.collection.immutable.Nil.:::
import scala.util.Random

object Words{

  def getInjuries() = {
    List("tchoin","catin","connasse","arriéré","biatch","bolos","bouffon","dushnok","enculé de ta race","enflure","enfoiré","garce","gouine","pd","trou du cul","couillon","salopard","nique ta mère","putain","salaud","salope","ta gueule","connard","enculé","idiot")
  }

  def getBasicsWords(): List[String] = {
    Storing.storingWords()
  }

  def getAllWords(): List[String] = {
    getBasicsWords ++ getInjuries
  }

  def getRandomWords(words : List[String]): String = {
    val rand = new Random()
    words(rand.between(0,words.length-1))
  }

  def getWords(age : Int, sex : String, club : String)={
    val rand = new Random()
    val hostilityProbability = rand.nextDouble()
    val hostilityRate = getHostilityCoefficient(age, sex, club)
    if (hostilityProbability <= hostilityRate){
      (1 to 9)
        .toList
        .map(x=>getRandomWords(getAllWords())) :+ getRandomWords(getInjuries())
    }
    else{
      (1 to 10)
        .toList
        .map(x=>getRandomWords(getAllWords()))
    }
  }

  def getHostilityCoefficient(age : Int, sex : String, club : String) : Double = {
    getAgeHostility(age) + getSexHostility(sex) + getClubHostility(club)
  }

  def getAgeHostility(age: Int): Double = age match{
    case _ if (age<= 25) => 0.2
    case _ if (age<= 35) => 0.1
    case _ => 0.0
  }

  def getSexHostility(sex: String): Double = sex match{
    case "M" => 0.1
    case _ => 0.0
  }

  def getClubHostility(club: String): Double = club match{
    case "Paris-Saint-Germain" => 0.3
    case "Bayern Munich" | "Manchester United" => 0.2
    case _ if (club != "None") => 0.1
    case _ => 0.0
  }
}
