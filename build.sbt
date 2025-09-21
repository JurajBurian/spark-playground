val v = new {
  val Scala          = "2.13.16"
  val Spark          = "4.0.1"
}

ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := v.Scala
ThisBuild / scalacOptions ++=
  Seq("-encoding", "UTF-8", "-unchecked", "-feature", "-explaintypes")

val coreSparkLibs = Seq(
  "org.apache.spark" %% "spark-core" % v.Spark % Provided,
  "org.apache.spark" %% "spark-sql"  % v.Spark % Provided
)
val commonLibs = Seq() // <- add extra libs if necessary in the future

lazy val `spark-examples-root` = (project in file("."))
  .settings(
    assembly / mainClass := Some("com.jubu.spark.Main"),
    libraryDependencies ++= coreSparkLibs ++ commonLibs ++ Seq(), // <- add extra libs if necessary in the future
    run / fork    := true,
    Compile / run := Defaults
      .runTask(Test / fullClasspath, Compile / run / mainClass, Compile / run / runner)
      .evaluated
  )

