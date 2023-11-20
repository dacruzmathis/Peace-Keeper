package Kafka

import Model.PeaceWatcher
import discord4j.core.DiscordClientBuilder
import discord4j.core.`object`.entity.channel.{GuildMessageChannel, MessageChannel}
import discord4j.core.event.domain.message.MessageCreateEvent
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import reactor.core.publisher.Mono

import java.time.Duration
import java.util.Properties


object ConsumerAlert extends App {
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "my-consumer-group")

  val topic = "filtered-topic"

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(java.util.Collections.singletonList(topic))

  val token = "MTEwODQ0MzkxOTQ2Njg5MzMzMg.GnphtH.4KrMosh8jZG139iFdAaFbqmB4Xz-600dacmRxU"
  val client = DiscordClientBuilder.create(token).build()
  val gateway = client.login().block()

  while (true) {
    val records = consumer.poll(Duration.ofMillis(100)).iterator()

    while (records.hasNext) {
      val record = records.next()
      val peaceWatcherJson = record.value()

      // affiche le contenu du JSON
      println("Json Data:")
      println(peaceWatcherJson)

      // Désérialiser l'objet PeaceWatcher à partir du JSON
      val peaceWatcherList = decode[List[PeaceWatcher]](peaceWatcherJson) match {
        case Right(value) => value
        case Left(error) => throw new RuntimeException("Erreur lors de la désérialisation de PeaceWatcher", error)
      }

      // utilise l'objet PeaceWatcher désérialisé récupéré
      println("After deserialization:")
      println(peaceWatcherList)

      gateway.on(classOf[MessageCreateEvent]).subscribe { event: MessageCreateEvent =>

        // boucle sur la list de PeaceWatcher
        peaceWatcherList.foreach { peaceWatcher =>
          // boucle sur la list de Person dans PeaceWatcher
          peaceWatcher.persons.foreach { person =>
            val personName = person.name
            val response = s"Peacewatcher ${peaceWatcher.id}: Score de la personne $personName est inférieur à 5 ! Location: (lat: ${peaceWatcher.location.latitude} , lon: ${peaceWatcher.location.longitude})"

            // Envoi notif discord
            val channel: Mono[MessageChannel] = event.getMessage.getChannel

            channel.cast(classOf[GuildMessageChannel])
              .flatMap(_.createMessage(response))
              .subscribe()
          }
        }
      }
    }
  }

  // Fermer le consommateur Kafka
  consumer.close()
}