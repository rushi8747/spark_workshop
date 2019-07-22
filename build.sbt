name := "spark_workshop"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

val sparkV = "2.2.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkV,
  "org.apache.spark" %% "spark-sql"       % sparkV,
  "org.apache.spark" %% "spark-hive"      % sparkV,
  "org.apache.spark" %% "spark-streaming" % sparkV,
  "org.apache.spark" %% "spark-mllib"     % sparkV
)

val scalatestV = "3.0.0-RC4"
libraryDependencies += "org.scalactic" %% "scalactic" % scalatestV
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestV % "test"
libraryDependencies += "org.json" % "json" % "20160810"