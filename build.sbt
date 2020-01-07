name := "UsersAndGroups"

version := "0.1"

scalaVersion := "2.13.0"



libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick"           % "3.3.2",
  "org.postgresql" % "postgresql" % "42.1.1",
  "ch.qos.logback"      % "logback-classic" % "1.2.3",
  //"ch.megard" %% "akka-http-cors" % "0.3.0",
  "com.zaxxer" % "HikariCP" % "2.4.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2",
  "javax.ws.rs" % "javax.ws.rs-api" % "2.0"
)
val akkaVersion = "2.5.23"
val akkaHttpVersion = "10.1.9"

libraryDependencies += "com.typesafe.akka" %% "akka-http"   % akkaHttpVersion
libraryDependencies += "com.typesafe.akka" %% "akka-actor"  % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.9"
// If testkit used, explicitly declare dependency on akka-streams-testkit in same version as akka-actor
libraryDependencies += "com.typesafe.akka" %% "akka-http-testkit"   % akkaHttpVersion % Test
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion     % Test
libraryDependencies += "net.codingwell" %% "scala-guice" % "4.2.6"
libraryDependencies += "com.github.swagger-akka-http" %% "swagger-akka-http" % "1.1.0"
libraryDependencies += "ch.megard" %% "akka-http-cors" % "0.4.1"
libraryDependencies += "io.pileworx" %% "akka-http-hal" % "1.2.5"
libraryDependencies += "org.gnieh" %% f"diffson-spray-json" % "4.0.1"
libraryDependencies += "org.typelevel" %% "cats-core" % "2.0.0"

enablePlugins(FlywayPlugin)
version := "0.0.1"
name := "flyway-sbt-test1"

flywayUrl := "jdbc:postgresql://localhost:5432/postgres"
flywayUser := "postgres"
flywayPassword := "1234567Nata"
flywayLocations += "db.migration"
flywayUrl in Test := "jdbc:postgresql://localhost:5432/postgres"
flywayUser in Test := "postgres"
flywayPassword in Test := "1234567Nata"