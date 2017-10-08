package com.test.newday.core

import com.test.newday.constants.{ArgumentsIndex}
import com.test.newday.output.SaveAsParquet
import com.test.newday.reports.{MovieRatings, UserFavouritesFilms}
import com.test.newday.tables.LoadTables
import org.apache.spark.sql.hive.HiveContext

class ProblemResolver(args:Array[String], sqlContext: HiveContext) {

  def run() = {
    val movies = LoadTables.load(sqlContext, args(ArgumentsIndex.moviesPath), LoadTables.movie)
    val ratings = LoadTables.load(sqlContext, args(ArgumentsIndex.ratingsPath), LoadTables.rating)
    val moviesRatings = MovieRatings.register(sqlContext, movies, ratings)
    val usersWith3Films = UserFavouritesFilms.register(sqlContext, movies, ratings, moviesRatings)

    SaveAsParquet.save(movies, args(ArgumentsIndex.moviesOutput))
    SaveAsParquet.save(ratings, args(ArgumentsIndex.ratingsOutput))
    SaveAsParquet.save(moviesRatings, args(ArgumentsIndex.moviesRatingsOutput))
    SaveAsParquet.save(usersWith3Films, args(ArgumentsIndex.usersWith3FilmsOutput))
  }

}
