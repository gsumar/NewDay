package com.test.newday.reports

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import scala.collection.mutable

object UserFavouritesFilms {

  def register(sqlContext:HiveContext, movies:DataFrame, ratings:DataFrame, movieRatings:DataFrame ) :  DataFrame = {

    movies.registerTempTable("movies")
    ratings.registerTempTable("ratings")
    movieRatings.registerTempTable("movieRatings")

    val limitAndFormatTitlesUDF = udf { (nums: mutable.WrappedArray[String], limit: Int) => nums.take(limit).map(x => x.split(" ")(1)) }

    val userFilms = sqlContext.sql("select r.userId as userId, m.title as title, mr.avg as avg, CONCAT(mr.avg, ' ' ,m.title) as value " +
      "from movies m inner join movieRatings mr on m.movieId = mr.movieId " +
      "inner join ratings r on m.movieId = r.movieId ")

    userFilms.registerTempTable("user_films")
    val fullListDF = sqlContext.sql("select userId, sort_array(collect_list(value), false) AS collected FROM user_films GROUP BY userId")
    val onlyThreeDF = fullListDF.withColumn("films", limitAndFormatTitlesUDF(fullListDF("collected"), lit(3)))
    val usersWith3Films = onlyThreeDF.select("userId", "films")
    usersWith3Films
  }
}
