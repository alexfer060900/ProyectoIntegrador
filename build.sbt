ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.10"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.3"
libraryDependencies += "org.json4s" %% "json4s-core" % "4.0.6"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "4.0.6"

lazy val root = (project in file("."))
  .settings(
    name := "primerAvance"

)
