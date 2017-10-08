// give the user a nice default project!
lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.test",
      scalaVersion := "2.10.4"
    )),
    name := "NewDay",
    version := "0.0.1",
    //sparkVersion := "1.6.1",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    //sparkComponents := Seq("core", "sql", "catalyst", "mllib"),
    parallelExecution in Test := false,
    fork := true,
    //coverageHighlighting := true,
    //spIgnoreProvided := true,
    mainClass in (Compile, run) := Some("com.test.newday.Prog"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.1" ,
      "org.scalacheck" %% "scalacheck" % "1.13.4" ,
      "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.7.2" ,
      "org.apache.spark" % "spark-core_2.10" % "1.6.1"  exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy") ,
      "org.apache.hadoop" % "hadoop-common" % "2.7.0"   exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy") excludeAll ExclusionRule(organization = "javax.servlet"),
      "org.apache.spark" % "spark-sql_2.10" % "1.6.1"   exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
      "org.apache.spark" % "spark-hive_2.10" % "1.6.1"  exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
      "org.apache.spark" % "spark-yarn_2.10" % "1.6.1"   exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy")
    ),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },
    resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),
    // publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )
