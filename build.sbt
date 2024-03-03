name := "crypticsdelta"

val sparkVersion: String = "3.3.2"

scalaVersion := "3.3.1"

libraryDependencies ++= Seq(
  "io.delta"            %% "delta-core"       % "2.3.0",
  "org.apache.spark"    %% "spark-core"       % sparkVersion, // % Provided,
  "org.apache.spark"    %% "spark-sql"        % sparkVersion, // % Provided,
  "org.scalameta"       %% "munit"            % "1.0.0-M11" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % Test
).map(_.cross(CrossVersion.for3Use2_13))

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*)        => MergeStrategy.discard
  case x: String                            => MergeStrategy.first
}

testFrameworks += new TestFramework("munit.Framework")
