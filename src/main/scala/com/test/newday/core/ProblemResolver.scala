package com.test.newday.core

import com.test.newday.constants.{ArgumentsIndex, TableType}
import com.test.newday.core.output.SaveAsParquet
import com.test.newday.core.reports.{MovieRatings, UserFavouritesFilms}
import com.test.newday.core.tables.LoadTables
import org.apache.spark.sql.hive.HiveContext

class ProblemResolver(args:Array[String], sqlContext: HiveContext) {

  def run() = {
    val movies = LoadTables.load(sqlContext, args(ArgumentsIndex.moviesPath), TableType.Movie)
    val ratings = LoadTables.load(sqlContext, args(ArgumentsIndex.ratingsPath), TableType.Rating)
    val moviesRatings = MovieRatings.register(sqlContext, movies, ratings)
    val usersWith3Films = UserFavouritesFilms.register(sqlContext, movies, ratings, moviesRatings)

    SaveAsParquet.save(movies, args(ArgumentsIndex.moviesOutput))
    SaveAsParquet.save(ratings, args(ArgumentsIndex.ratingsOutput))
    SaveAsParquet.save(moviesRatings, args(ArgumentsIndex.moviesRatingsOutput))
    SaveAsParquet.save(usersWith3Films, args(ArgumentsIndex.usersWith3FilmsOutput))
  }

}
