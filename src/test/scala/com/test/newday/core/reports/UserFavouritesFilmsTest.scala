package com.test.newday.core.reports

import com.test.newday.constants.TableType
import com.test.newday.core.tables.LoadTables
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.{FlatSpec, Matchers}

class UserFavouritesFilmsTest extends FlatSpec with Matchers {

  "Testing a movie ratings aggregations" should "return 4 lines" in {
    val movies = LoadTables.load(TestHive,
      "src/test/resources/sources/movies.dat", TableType.Movie)

    val ratings = LoadTables.load(TestHive,
      "src/test/resources/sources/ratings.dat", TableType.Rating)

    val movieRatings = MovieRatings.register(TestHive, movies, ratings)

    assert(UserFavouritesFilms.register(TestHive,movies,ratings,movieRatings).count() == 2)
  }
}