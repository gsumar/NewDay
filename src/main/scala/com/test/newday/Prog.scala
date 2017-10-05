package com.test.NewDay

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

object Prog {

  case class Movie (movieId:Int, title:String, genre:String)
  case class Rating (userId:Int, movieId:Int, rating:Int, timestamp:Int)
  case class User (userId:Int, genge:String, age:Int, Occupation:Int, zipCode:String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NewDay exercise").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val fieldsTerminatedBy = "::"

    // problem 1

    val pathMovies = args(0)
    val moviesRDD = sc.textFile(pathMovies)
    val moviesMap = moviesRDD.map(x => x.split(fieldsTerminatedBy)).map(x => Movie(x(0).toInt, x(1), x(2)))
    val movies = moviesMap.toDF

    val pathRatings = args(1)
    val ratingsRDD = sc.textFile(pathRatings)
    val ratingsMap = ratingsRDD.map(x => x.split(fieldsTerminatedBy)).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt))
    val ratings = ratingsMap.toDF

    val pathUsers = args(2)
    val usersRDD = sc.textFile(pathUsers)
    val usersMap = usersRDD.map(x => x.split(fieldsTerminatedBy)).map(x => User(x(0).toInt, x(1), x(2).toInt, x(3).toInt, x(4)))
    val users = usersMap.toDF

    // problem 2

    movies.registerTempTable("movies")
    ratings.registerTempTable("ratings")
    users.registerTempTable("users")

    val movieRatings = sqlContext.sql("SELECT m.*, max(r.rating) as max, min(r.rating) as min, avg(r.rating) as avg FROM movies m INNER JOIN ratings r ON m.movieId = r.movieId " +
      "GROUP BY m.movieId, m.title, m.genre")


    // problem 3

    val limitAndFormatTitlesUDF = udf { (nums: mutable.WrappedArray[String], limit: Int) => nums.take(limit).map(x => x.split(" ")(1)) }
    movieRatings.registerTempTable("movieRatings")

    sqlContext.sql("select r.userId as userId, m.title as title, mr.avg as avg, CONCAT(mr.avg, ' ' ,m.title) as value " +
      "from movies m inner join movieRatings mr on m.movieId = mr.movieId " +
      "inner join ratings r on m.movieId = r.movieId ").registerTempTable("user_films")
    val fullListDF = sqlContext.sql("select userId, sort_array(collect_list(value), false) AS collected FROM user_films GROUP BY userId")
    val onlyThreeDF = fullListDF.withColumn("films", limitAndFormatTitlesUDF(fullListDF("collected"), lit(3)))
    val usersWith3Films = onlyThreeDF.select("userId", "films")

    //problem4
    movies.write.parquet(args(3))
    ratings.write.parquet(args(4))
    users.write.parquet(args(5))
    movieRatings.write.parquet(args(6))
    usersWith3Films.write.parquet(args(7))
  }

}