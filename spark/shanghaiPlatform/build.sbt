name := "TelecomDataAnalysisWithKafka"

version := "1.2"

scalaVersion := "2.10.4"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.4" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"

<<<<<<< HEAD:spark/shanghaiPlatform/build.sbt
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1"
=======
resolvers += "Kunyan Repo" at "http://222.73.34.92:8081/nexus/content/groups/public/"

libraryDependencies += "com.kunyan" % "nlpsuit-package" % "0.2.3"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.4" % "provided"
>>>>>>> 1c673e274b979eb50f931b2c81c6698ef600de0a:spark/hotwords/build.sbt

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

libraryDependencies += "org.json" % "json" % "20140107"

libraryDependencies += "com.ibm.icu" % "icu4j" % "56.1"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "scala-lang", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
    
