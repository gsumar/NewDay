package com.test.newday.core.reports

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext


object MovieRatings {

  def register(sqlContext:HiveContext, movies:DataFrame, ratings:DataFrame) : DataFrame =  {
    movies.registerTempTable("movies")
    ratings.registerTempTable("ratings")
    sqlContext.sql("SELECT m.*, max(r.rating) as max, min(r.rating) as min, avg(r.rating) as avg FROM movies m INNER JOIN ratings r ON m.movieId = r.movieId " +
      "GROUP BY m.movieId, m.title, m.genre")
  }
}
