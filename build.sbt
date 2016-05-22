name := "spark-workshop"
scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-hive" % "1.6.1"
)
