name := "Pe_minirun"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.commons" % "commons-email" % "1.5",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.typesafe" % "config" % "1.2.1",
  "mysql" % "mysql-connector-java" % "8.0.17",
  "net.liftweb" %% "lift-json" % "3.1.1",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "8.1.0"
)
