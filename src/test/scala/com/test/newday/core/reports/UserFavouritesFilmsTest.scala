package com.test.newday.core.reports

import com.test.newday.core.tables.{BasicTableLoader, BasicTableType}
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.{FlatSpec, Matchers}

class UserFavouritesFilmsTest extends FlatSpec with Matchers {

  "Testing a movie ratings aggregations" should "return 4 lines" in {
    val movies = BasicTableLoader.load(
      "src/test/resources/sources/movies.dat", BasicTableType.Movie)

    val ratings = BasicTableLoader.load(
      "src/test/resources/sources/ratings.dat", BasicTableType.Rating)

    val movieRatings = MovieRatings.register(movies, ratings)

    assert(UserFavouritesFilms.register(movies,ratings,movieRatings).count() == 2)
  }
}