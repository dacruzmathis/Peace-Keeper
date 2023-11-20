package Kafka

import Model.PeaceWatcher
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

import java.util.Properties

object Stream extends App {

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "peacewatcher-stream")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())

  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "latest")

  val builder = new StreamsBuilder()

  val inputTopic = "atopic"
  val outputTopic = "filtered-topic" // Topic vers lequel les données filtrées seront envoyées

  val stream: KStream[String, String] = builder.stream(inputTopic)

  stream.mapValues((_, value) => processValue(value)).to(outputTopic, Produced.`with`(Serdes.String(), Serdes.String()))

  val topology = builder.build()
  val streams = new KafkaStreams(topology, props)
  streams.start()

  sys.addShutdownHook {
    streams.close()
  }



  def processValue(value: String): String = {
    val decoded = decode[List[PeaceWatcher]](value)
    decoded match {
      case Right(peaceWatchers) =>
        val filteredPeaceWatchers = peaceWatchers.flatMap(filterPeaceWatcher)
        filteredPeaceWatchers.asJson.noSpaces

      case Left(error) =>
       s"Failed to decode JSON: $error"
    }
  }

  def filterPeaceWatcher(pw: PeaceWatcher): Option[PeaceWatcher] = {
    val filteredPersons = pw.persons.filter(_.score < 5)
    if (filteredPersons.nonEmpty) {
      val filteredPeaceWatcher = pw.copy(persons = filteredPersons)
      Some(filteredPeaceWatcher)
    } else {
      None
    }
  }
}