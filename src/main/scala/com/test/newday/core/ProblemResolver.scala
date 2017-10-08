package com.test.newday.core

import com.test.newday.common.constants.ArgumentsIndex
import com.test.newday.core.output.ParquetSaver
import com.test.newday.core.reports.{MovieRatings, UserFavouritesFilms}
import com.test.newday.core.tables.{BasicTableLoader, BasicTableType}

class ProblemResolver(args:Array[String]) {

  def run() = {
    val movies = BasicTableLoader.load(args(ArgumentsIndex.moviesPath), BasicTableType.Movie)
    val ratings = BasicTableLoader.load(args(ArgumentsIndex.ratingsPath), BasicTableType.Rating)
    val moviesRatings = MovieRatings.register(movies, ratings)
    val usersWith3Films = UserFavouritesFilms.register(movies, ratings, moviesRatings)

    ParquetSaver.save(movies, args(ArgumentsIndex.moviesOutput))
    ParquetSaver.save(ratings, args(ArgumentsIndex.ratingsOutput))
    ParquetSaver.save(moviesRatings, args(ArgumentsIndex.moviesRatingsOutput))
    ParquetSaver.save(usersWith3Films, args(ArgumentsIndex.usersWith3FilmsOutput))
  }

}
