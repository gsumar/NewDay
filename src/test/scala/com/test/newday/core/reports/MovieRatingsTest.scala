package com.test.newday.core.reports

import com.test.newday.core.tables.{BasicTableLoader, BasicTableType}
import com.test.newday.environment.NewDayContext
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.{FlatSpec, Matchers}

class MovieRatingsTest extends FlatSpec with Matchers {

  NewDayContext.init(TestHive)

  "Testing a movie ratings aggregations" should "return 4 lines" in {
    val movies = BasicTableLoader.load(
      "src/test/resources/sources/movies.dat", BasicTableType.Movie)

    val ratings = BasicTableLoader.load(
      "src/test/resources/sources/ratings.dat", BasicTableType.Rating)

    assert(MovieRatings.register(movies, ratings).count() == 4)
  }
}