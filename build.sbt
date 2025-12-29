val scala3Version = "3.7.4"

lazy val root = project
  .in(file("."))
  .settings(
    name := "rokim_db",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "2.12.0",
      "org.typelevel" %% "cats-effect" % "3.5.7",
      "co.fs2" %% "fs2-core" % "3.11.0",
      "co.fs2" %% "fs2-io" % "3.11.0",
      "org.scalameta" %% "munit" % "1.0.0" % Test
    )
  )
