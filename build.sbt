import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / scalaVersion := "2.13.8"

ThisBuild / organization := "org.cameron.cs"

ThisBuild / organizationName := "spark-stream-dynamic-workflow"

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Xfatal-warnings"
)

lazy val sparkVersion            = Dependencies.sparkVersion
lazy val hadoopVersion           = Dependencies.hadoopVersion
lazy val kafkaVersion            = Dependencies.kafkaVersion
lazy val scoptVersion            = Dependencies.scoptVersion
lazy val prometheusClientVersion = Dependencies.prometheusClient
lazy val log4jVersion            = Dependencies.log4j
lazy val scalatestVersion        = Dependencies.scalatest
lazy val scalacheckVersion       = Dependencies.scalacheck

lazy val projectSettings = Seq(
  version := "1.0.0",
  unmanagedBase := baseDirectory.value / "lib",
  resolvers ++= Seq(
    Resolver.mavenLocal,
    Resolver.sonatypeRepo("releases"),
    Resolver.DefaultMavenRepository
  ),
  resourceDirectory in Compile := baseDirectory.value.getParentFile / "data",
    libraryDependencies ++= Seq(
    ("org.apache.spark" %% "spark-core"                          % sparkVersion  % Provided)
      .exclude("org.slf4j", "slf4j-api")
      .exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.spark"  %% "spark-mllib"                         % sparkVersion      % Provided,
    "org.apache.spark"  %% "spark-hive"                          % sparkVersion      % Provided,
    "org.apache.spark"  %% "spark-sql"                           % sparkVersion      % Provided,
    "org.apache.spark"  %% "spark-catalyst"                      % sparkVersion      % Provided,
    "org.apache.spark"  %% "spark-sql-kafka-0-10"                % sparkVersion      % Provided,
    "org.apache.spark"  %% "spark-streaming"                     % sparkVersion      % Provided,
    "org.apache.spark"  %% "spark-yarn"                          % sparkVersion      % Provided,
    "org.apache.hadoop" %  "hadoop-client-runtime"               % hadoopVersion     % Provided,
    "org.apache.hadoop" %  "hadoop-client-api"                   % hadoopVersion     % Provided,

    ("org.apache.kafka" % "kafka-clients"                        % kafkaVersion).exclude("com.fasterxml.jackson.core", "*"),
    ("org.apache.kafka" %% "kafka"                               % kafkaVersion).exclude("com.fasterxml.jackson.core", "*"),
    // additional dependencies
    "com.github.scopt"               %% "scopt"                  % scoptVersion,
    "io.prometheus"                  % "simpleclient"            % prometheusClientVersion,
    "io.prometheus"                  % "simpleclient_common"     % prometheusClientVersion,
    "io.prometheus"                  % "simpleclient_httpserver" % prometheusClientVersion,
    // updated logging dependencies for Spark 3.x
    "org.apache.logging.log4j" % "log4j-api"                     % log4jVersion,
    "org.apache.logging.log4j" % "log4j-core"                    % log4jVersion,
    "org.apache.logging.log4j" % "log4j-slf4j2-impl"             % log4jVersion
  )
)

lazy val testSettings = Seq(
  Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test",
  libraryDependencies += "org.scalatest"  %% "scalatest"         % scalatestVersion  % Test,
  libraryDependencies += "org.scalacheck" %% "scalacheck"        % scalacheckVersion % Test,
  testFrameworks += new TestFramework("org.scalatest.tools.Framework")
)

lazy val common = (project in file("common"))
  .settings(projectSettings)
  .settings(
    organization := "org.cameron.cs",
    name := "common",
    version := "1.0",
    assembly / assemblyJarName := "common.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.endsWith(".properties") => MergeStrategy.concat
      case x if x.endsWith(".conf") => MergeStrategy.concat
      case x if x.endsWith(".xml") => MergeStrategy.concat
      case x if x.contains("Log4j2Plugins.dat") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

lazy val blogsStream = (project in file("blogs_stream_dynamic_workflow"))
  .settings(projectSettings)
  .settings(testSettings)
  .dependsOn(common)
  .settings(
    organization := "org.cameron.cs",
    name := "blogs-stream",
    version := "1.0",
    assembly / assemblyJarName := "blogs_stream_dynamic_workflow.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.endsWith(".properties") => MergeStrategy.concat
      case x if x.endsWith(".conf") => MergeStrategy.concat
      case x if x.endsWith(".xml") => MergeStrategy.concat
      case x if x.contains("Log4j2Plugins.dat") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

lazy val metricsStream = (project in file("metrics_stream_dynamic_workflow"))
  .settings(projectSettings)
  .settings(testSettings)
  .dependsOn(common)
  .settings(
    organization := "org.cameron.cs",
    name := "metrics-stream-dynamic-workflow",
    version := "1.0",
    assembly / assemblyJarName := "metrics_stream_dynamic_workflow.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.endsWith(".properties") => MergeStrategy.concat
      case x if x.endsWith(".conf") => MergeStrategy.concat
      case x if x.endsWith(".xml") => MergeStrategy.concat
      case x if x.contains("Log4j2Plugins.dat") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

lazy val postsStream = (project in file("posts_stream_dynamic_workflow"))
  .settings(projectSettings)
  .settings(testSettings)
  .dependsOn(common)
  .settings(
    organization := "org.cameron.cs",
    name := "posts-stream-dynamic-workflow",
    version := "1.0",
    assembly / assemblyJarName := "posts_stream_dynamic_workflow.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.endsWith(".properties") => MergeStrategy.concat
      case x if x.endsWith(".conf") => MergeStrategy.concat
      case x if x.endsWith(".xml") => MergeStrategy.concat
      case x if x.contains("Log4j2Plugins.dat") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

lazy val streamsMerger = (project in file("streams_merger_dynamic_workflow"))
  .settings(projectSettings)
  .settings(testSettings)
  .dependsOn(common, blogsStream, metricsStream, postsStream)
  .settings(
    organization := "org.cameron.cs",
    name := "stream-merger-dynamic-workflow",
    version := "1.0",
    assembly / assemblyJarName := "streams_dynamic_workflow_merger.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.endsWith(".properties") => MergeStrategy.concat
      case x if x.endsWith(".conf") => MergeStrategy.concat
      case x if x.endsWith(".xml") => MergeStrategy.concat
      case x if x.contains("Log4j2Plugins.dat") => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

// root project
lazy val root =
  (project in file("."))
    .enablePlugins(AssemblyPlugin)
    .aggregate(common, blogsStream, metricsStream, postsStream, streamsMerger)
    .settings(
      name := "spark-stream-dynamic-workflow",
      Test / skip := false
    )

enablePlugins(AssemblyPlugin)
