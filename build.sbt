name := "spark-workshop"
scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided",
  "com.github.melrief" %% "purecsv" % "0.0.6",
  compilerPlugin("org.scalamacros" % "paradise" % "2.0.1" cross CrossVersion.full)
)

assemblyJarName in assembly := "fat.jar"
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)) 
