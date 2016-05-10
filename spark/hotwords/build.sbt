name := "hotwords"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Kunyan Repo" at "http://222.73.34.92:8081/nexus/content/groups/public/"

resolvers += "OSChina" at "http://maven.oschina.net/content/groups/public/"

resolvers += "Kunyan Repo" at "http://222.73.34.92:8081/nexus/content/groups/public/"

libraryDependencies += "com.kunyan" % "nlpsuit" % "0.2"

libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.10.4" % "provided"

libraryDependencies += "net.databinder.dispatch" % "dispatch-core_2.10" % "0.11.0"

libraryDependencies += "com.ning" % "async-http-client" % "1.7.16"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2" excludeAll ExclusionRule(organization = "org.mortbay.jetty")

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

libraryDependencies += "org.wltea.ik-analyzer" % "ik-analyzer" % "3.2.8"

libraryDependencies += "redis.clients" % "jedis" % "2.8.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38" % "provided"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "el", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
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


test in assembly := {}