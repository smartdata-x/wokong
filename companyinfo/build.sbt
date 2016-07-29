
name := "companyinfo"

version := "1.0"

scalaVersion := "2.10.4"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers += "Kunyan Repo" at "http://222.73.34.92:8081/nexus/content/groups/public/"

libraryDependencies += "com.kunyan" % "nlpsuit-package" % "0.2.6.6"

libraryDependencies += "org.jsoup" % "jsoup" % "1.8.3"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"

libraryDependencies += "com.ibm.icu" % "icu4j" % "56.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2" exclude("org.apache.spark", "spark-streaming_2.10")

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "redis.clients" % "jedis" % "2.8.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "net.minidev" % "json-smart" % "1.0.9"

