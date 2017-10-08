package com.test.newday.core.reports

import com.test.newday.environment.NewDayContext
import org.apache.spark.sql.DataFrame


object MovieRatings {

  def register(movies:DataFrame, ratings:DataFrame) : DataFrame =  {
    movies.registerTempTable("movies")
    ratings.registerTempTable("ratings")
    NewDayContext.sqlContext.sql("SELECT m.*, max(r.rating) as max, min(r.rating) as min, avg(r.rating) as avg FROM movies m INNER JOIN ratings r ON m.movieId = r.movieId " +
      "GROUP BY m.movieId, m.title, m.genre")
  }
}
