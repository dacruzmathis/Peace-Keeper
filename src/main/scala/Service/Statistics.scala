package Service

import Model.Words
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{FileOutputStream, PrintStream}

object Statistics extends App {
  val conf: SparkConf = new SparkConf()
    .setAppName("Statistics")
    .setMaster("local[*]")

  val ss: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()


  def getMarkByNIteration(numberIterations: Int = -1): DataFrame = {

    val directoryPath = "/scalaProject"

    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:9000")

    val fs = FileSystem.get(conf)
    val fileStatuses = fs.listStatus(new Path(directoryPath))
    val recentFiles = if (numberIterations != -1) {
      fileStatuses
        .sortBy(_.getModificationTime)
        .take(numberIterations)
        .map(_.getPath.toString)
    }
    else
      {
        fileStatuses
          .sortBy(_.getModificationTime)
          .map(_.getPath.toString)
      }


    val loadDF_n_iterations = ss.read.format("json").load(recentFiles: _*)


    loadDF_n_iterations
  }

  def reportOutput(numberIterations: Int): Unit = {
    val reportAggregated = getMarkByNIteration(10)

    println(s"Average by $numberIterations reports for all PeaceWatcher")
    reportAggregated
      .withColumn("avgscore", expr("aggregate(persons.score, CAST(0.0 AS double), (acc, x) -> acc + x, acc -> acc/ size( persons.score ))"))
      .agg(avg(col("avgscore")))
      .show()

    println(s"Average by $numberIterations reports for each PeaceWatcher")
    reportAggregated
      .withColumn("avgscore", expr("aggregate(persons.score, CAST(0.0 AS double), (acc, x) -> acc + x, acc -> acc/ size( persons.score ))"))
      .groupBy(col("id"))
      .agg(avg(col("avgscore")))
      .sort(col("id"))
      .show()

    println(s"Average by $numberIterations reports for each report")

    reportAggregated
      .withColumn("avgscore", expr("aggregate(persons.score, CAST(0.0 AS double), (acc, x) -> acc + x, acc -> acc/ size( persons.score ))"))
      .groupBy(col("time"))
      .agg(avg(col("avgscore")))
      .sort(col("time"))
      .show()
    println(s"Top 5 of words the most repeated")

    reportAggregated
      .withColumn("exploded_persons", explode(col("persons")))
      .withColumn("exploded_words", explode(col("exploded_persons.words")))
      .select(col("exploded_words"))
      .groupBy(col("exploded_words"))
      .agg(count(col("exploded_words")).as("count"))
      .sort(desc("count"))
      .limit(5)
      .show()

    println(s"The person the most pleasant of the whole report")

    lazy val emptySchema = StructType(Seq(
      StructField("time", StringType),
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("count", IntegerType)
    ))
    lazy val emptyDF: DataFrame = ss.createDataFrame(ss.sparkContext.emptyRDD[Row], emptySchema)

    lazy val df = reportAggregated

    lazy val groupedDF = df
      .select(col("time"), explode(col("persons.score")).alias("score"))
      .groupBy("time")

    val maxScores = groupedDF
      .agg(max("score").alias("max_score"))
      .select("time", "max_score")
      .collect()

    val newUnionDF = maxScores.foldLeft(emptyDF) { (acc, row) =>
      val time = row.getAs[String]("time")
      val maxScore = row.getAs[Long]("max_score")

      val personsMostPleasant = df
        .select(col("time"), explode(col("persons")).alias("persons"))
        .filter(col("time") === time && col("persons.score") === maxScore)
        .select("time", "persons.id", "persons.name")
        .withColumn("count", lit(1))

      acc.union(personsMostPleasant)
    }

    println(newUnionDF.groupBy("id", "name").agg(sum("count").as("maxScoreAllReports"))
      .sort(desc("maxScoreAllReports"))
      .first())

    println("Most hostile area watched by PeaceWatcher for last report")

    val maxDateColumn = reportAggregated
      .select(max(col("time")))
      .first()
      .getAs[String](0)


    val peaceWatcherMostHostileLastReport: Unit = {
      lazy val explodedDF = reportAggregated
        .withColumn("person", explode(col("persons")))
      lazy val minScore = explodedDF.filter(col("time") === maxDateColumn).select(min(col("person.score"))).first().getLong(0)
      lazy val personsWithMinScore = explodedDF.filter(col("time") === maxDateColumn && col("person.score") === minScore)
      lazy val dfMax = personsWithMinScore.groupBy(col("id")).agg(count("id").as("count"))
      println(dfMax.sort(desc("count")).first())
    }

    println("Injury most repeated for all reports")

    println(reportAggregated
      .withColumn("exploded_persons", explode(col("persons")))
      .withColumn("exploded_words", explode(col("exploded_persons.words")))
      .filter(col("exploded_words").isin(Words.getInjuries(): _*))
      .groupBy(col("exploded_words"))
      .agg(count(col("exploded_words")).as("count"))
      .sort(desc("count"))
      .first())

    println("Average for each club in the last report")

    reportAggregated
      .withColumn("person", explode(col("persons")))
      .filter(col("time") === maxDateColumn)
      .select("person.club", "person.score")
      .groupBy("club")
      .agg(avg("score").as("average_score"))
      .show()

    println("Average for each age in the last report")

    reportAggregated
      .withColumn("person", explode(col("persons")))
      .filter(col("time") === maxDateColumn)
      .select("person.age", "person.score")
      .groupBy("age")
      .agg(avg("score").as("average_score"))
      .orderBy("age")
      .show(64)

    println("Average for a male in the last report")

    reportAggregated
      .withColumn("person", explode(col("persons")))
      .filter(col("time") === maxDateColumn)
      .filter("person.sex = 'M'")
      .groupBy()
      .agg(avg("person.score").as("average_score"))
      .show()

    println("Average for a female in the last report")

    reportAggregated
      .withColumn("person", explode(col("persons")))
      .filter(col("time") === maxDateColumn)
      .filter("person.sex = 'F'")
      .groupBy()
      .agg(avg("person.score").as("average_score"))
      .show()


  }

  // Define the output file path
  val outputPath = "src/main/scala/Service/evaluationReport"

  // Write the result string to a text file
  // Create a new PrintStream to write to the file
  val fileOutputStream = new FileOutputStream(outputPath)
  val printStream = new PrintStream(fileOutputStream)

  // Redirect the console output to the file
  Console.withOut(printStream) {
    reportOutput(10)
  }

  // Close the streams
  printStream.close()
  fileOutputStream.close()

}
