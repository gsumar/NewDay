package com.test.newday.core

import com.test.newday.common.constants.Arguments
import com.test.newday.core.output.ParquetSaver
import com.test.newday.core.reports.{MovieRatings, UserFavouritesFilms}
import com.test.newday.core.tables.{BasicTableLoader, BasicTableType}

class ProblemResolver(args:Array[String]) {

  def run() = {
    val movies = BasicTableLoader.load(args(Arguments.MoviesPath), BasicTableType.Movie)
    val ratings = BasicTableLoader.load(args(Arguments.RatingsPath), BasicTableType.Rating)
    val moviesRatings = MovieRatings.register(movies, ratings)
    val usersWith3Films = UserFavouritesFilms.register(movies, ratings, moviesRatings)

    ParquetSaver.save(movies, args(Arguments.MoviesOutput))
    ParquetSaver.save(ratings, args(Arguments.RatingsOutput))
    ParquetSaver.save(moviesRatings, args(Arguments.MoviesRatingsOutput))
    ParquetSaver.save(usersWith3Films, args(Arguments.UsersWith3FilmsOutput))
  }

}
