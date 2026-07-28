ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

fork := true
lazy val root = (project in file("."))
  .settings(
    name := "producer"
  )

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "com.typesafe" % "config" % "1.4.3",
  "org.scalafx" %% "scalafx" % "16.0.0-R25"
)
lazy val javaFXModules = Seq("base", "controls", "fxml", "graphics", "media", "swing", "web")

lazy val osName = System.getProperty("os.name") match {
  case n if n.startsWith("Linux")   => "linux"
  case n if n.startsWith("Mac")     => "mac"
  case n if n.startsWith("Windows") => "win"
  case _ => throw new Exception("OS non reconnu")
}

libraryDependencies ++= javaFXModules.map { m =>
  "org.openjfx" % s"javafx-$m" % "15.0.1" classifier osName
}
libraryDependencies += "com.sksamuel.scrimage" % "scrimage-core" % "4.3.0"
libraryDependencies += "com.microsoft.onnxruntime" % "onnxruntime" % "1.18.0"