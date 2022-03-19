name := "sparkles"
version := "0.1.0"
scalaVersion := "2.12.10"

libraryDependencies ++= Seq (
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.1",
  // "org.json4s" %% "json4s-jackson" % "3.6.7",
  "net.liftweb" %% "lift-json" % "3.3.0",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % Test,
  "org.apache.spark" %% "spark-core" % "3.2.1" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.2.1" % Provided,
  "org.apache.hadoop" % "hadoop-common" % "2.9.2" % Provided,
  "org.apache.hadoop" % "hadoop-hdfs-client" % "2.9.2" % Provided,
  "com.google.guava" % "guava" % "30.1.1-jre" % Provided
)

// excludeDependencies += "org.slf4j" % "slf4j-log4j12"

semanticdbEnabled := true
semanticdbVersion := scalafixSemanticdb.revision
scalacOptions += "-Ywarn-unused"
ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"

val consoleDisabledOptions = Seq("-Xfatal-warnings", "-Ywarn-unused", "-Ywarn-unused-import")
scalacOptions in (Compile, console) ~= (_ filterNot consoleDisabledOptions.contains)

// Compile / doc / scalacOptions := Seq("-private")

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}