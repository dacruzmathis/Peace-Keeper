package Kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import scala.collection.JavaConverters._
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.{Collections, Properties}

object ConsumerStorage extends App {

  val spark = SparkSession.builder()
    .appName("Write JSON to HDFS")
    .master("local")
    .getOrCreate()

  // Configuration du consommateur Kafka
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "my-consumer-group")

  val topic = "atopic"

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Collections.singletonList(topic))

  while (true) {
    val records = consumer.poll(java.time.Duration.ofMillis(10000))

    records.asScala foreach  { record =>
      val peaceWatcherJson = record.value()



      // Afficher le contenu du JSON
      println("Json Data:")
      println(peaceWatcherJson)

      val rddData = spark.sparkContext.parallelize(Seq(peaceWatcherJson))

      val df: DataFrame = spark.read.json(spark.createDataset(rddData)(Encoders.STRING))

      consumer.pause(consumer.assignment())
      try Thread.sleep(5000)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
      consumer.resume(consumer.assignment())

      val now = Instant.now()
      val formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH-mm-ss").withZone(ZoneId.systemDefault())
      val formattedInstant: String = formatter.format(now)
      val outputDirectory = "hdfs://localhost:9000/scalaProject/report_" + formattedInstant

      df.write.json(outputDirectory)

    }
  }

  // Fermer le consommateur Kafka
  consumer.close()
}
