//import sbt.Resolver.mavenLocal
//
//
//resolvers ++= Seq(
//  mavenLocal,
//  "Restlet Repository" at "http://maven.restlet.org/",
//  "JBoss Repository" at "https://repository.jboss.org/nexus/content/repositories/",
//  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
//  "Scala-Tools Snapshots" at "http://scala-tools.org/repo-snapshots/")

lazy val commonSettings = Seq(
  organization := "com.reljicd",
  version := "0.1",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "spark-learning",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.11" % "2.2.0",
      "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
      "com.github.tototoshi" %% "scala-csv" % "1.3.5",
      "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test",
      "org.apache.hadoop" % "hadoop-aws" % "2.6.0" % "provided",
      "org.apache.spark" %% "spark-hive" % "2.0.0" % "provided"))
