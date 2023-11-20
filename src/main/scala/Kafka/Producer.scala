package Kafka

import Model.PeaceWatcher
import Model.PeaceWatcher.makeIterations
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object Producer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  val topic = "atopic"

  val producer = new KafkaProducer[String, String](props)

  val listOfPw = PeaceWatcher.getInitialsPeaceWatchers

  // Convertir l'objet PeaceWatcher en JSON
  val iterations: LazyList[List[PeaceWatcher]] = makeIterations(listOfPw)
  iterations.foreach { pwList =>
    val peaceWatcherJson = pwList.asJson.noSpaces.replace("\r", "")

    // envoi message à kafka
    val record = new ProducerRecord[String, String](topic, peaceWatcherJson)
    producer.send(record)
    println(peaceWatcherJson)


  }

  // Fermer le producteur Kafka
  producer.close()
}
