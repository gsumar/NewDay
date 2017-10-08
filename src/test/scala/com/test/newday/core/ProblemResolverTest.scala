package com.test.newday.core

import com.test.newday.core.tables.{BasicTableLoader, BasicTableType}
import com.test.newday.environment.NewDayContext
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.{FlatSpec, Matchers}

class ProblemResolverTest extends FlatSpec with Matchers {

  NewDayContext.init(TestHive)

  val args:Array[String] = Array("src/test/resources/sources/movies.dat",
  "src/test/resources/sources/ratings.dat",
  "target/parquet/movies/",
    "target/parquet/ratings/",
    "target/parquet/movieRatings/",
    "target/parquet/userFavoritesFilms/")

  "Integration test" should "run OK" in {
    val problemResolver = new ProblemResolver(args)
    problemResolver.run()
  }
}