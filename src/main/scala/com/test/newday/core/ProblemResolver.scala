package com.test.newday.core

import com.test.newday.common.constants.ArgumentsIndex
import com.test.newday.core.output.ParquetSaver
import com.test.newday.core.reports.{MovieRatings, UserFavouritesFilms}
import com.test.newday.core.tables.{BasicTableLoader, BasicTableType}
import org.apache.spark.sql.hive.HiveContext

class ProblemResolver(args:Array[String], sqlContext: HiveContext) {

  def run() = {
    val movies = BasicTableLoader.load(sqlContext, args(ArgumentsIndex.moviesPath), BasicTableType.Movie)
    val ratings = BasicTableLoader.load(sqlContext, args(ArgumentsIndex.ratingsPath), BasicTableType.Rating)
    val moviesRatings = MovieRatings.register(sqlContext, movies, ratings)
    val usersWith3Films = UserFavouritesFilms.register(sqlContext, movies, ratings, moviesRatings)

    ParquetSaver.save(movies, args(ArgumentsIndex.moviesOutput))
    ParquetSaver.save(ratings, args(ArgumentsIndex.ratingsOutput))
    ParquetSaver.save(moviesRatings, args(ArgumentsIndex.moviesRatingsOutput))
    ParquetSaver.save(usersWith3Films, args(ArgumentsIndex.usersWith3FilmsOutput))
  }

}
