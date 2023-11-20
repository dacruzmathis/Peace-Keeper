ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.2"
libraryDependencies += "io.circe" %% "circe-core" % "0.14.5"
libraryDependencies += "io.circe" %% "circe-generic" % "0.14.5"
libraryDependencies += "io.circe" %% "circe-parser" % "0.14.5"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "3.0.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.0.0"
libraryDependencies += "com.discord4j" % "discord4j-core" % "3.1.1"

lazy val root = (project in file("."))
  .settings(
    name := "PeaceKeeper"
  )
