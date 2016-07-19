mainClass in (Compile, packageBin) := Some("SparkDriver")

name := "search_spark"

version := "1.8"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.5.2"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.5" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "org.datanucleus" % "datanucleus-core" % "3.2.10" % "provided"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "plugin.xml" => MergeStrategy.rename
  case "overview.html" => MergeStrategy.rename
  case "parquet.thrift" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

