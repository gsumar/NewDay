package com.test.newday.reports

import com.test.newday.constants.TableType
import com.test.newday.tables.LoadTables
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.{FlatSpec, Matchers}

class MovieRatingsTest extends FlatSpec with Matchers {

  "Testing a movie ratings aggregations" should "return 4 lines" in {
    val movies = LoadTables.load(TestHive,
      "src/test/resources/sources/movies.dat", TableType.Movie)

    val ratings = LoadTables.load(TestHive,
      "src/test/resources/sources/ratings.dat", TableType.Rating)

    assert(MovieRatings.register(TestHive, movies, ratings).count() == 4)
  }
}