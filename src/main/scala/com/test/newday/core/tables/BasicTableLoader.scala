package com.test.newday.core.tables

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object BasicTableLoader {


  private val fieldsTerminatedBy = "::"

  private case class Movie (movieId:Int, title:String, genre:String)
  private case class Rating (userId:Int, movieId:Int, rating:Int, timestamp:Int)
  private case class User (userId:Int, genge:String, age:Int, Occupation:Int, zipCode:String)

  def load(sqlContext:HiveContext, inputPath: String, table:BasicTableType.EnumVal): DataFrame = {
    import sqlContext.implicits._
    val rdd = sqlContext.sparkContext.textFile(inputPath)
    val rddSplit = rdd.map(x => x.split(fieldsTerminatedBy))

    table match  {
      case BasicTableType.Movie => rddSplit.map(x => Movie(x(0).toInt, x(1), x(2))).toDF()
      case BasicTableType.Rating => rddSplit.map(x => Rating(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt)).toDF()
      case BasicTableType.User => rddSplit.map(x => User(x(0).toInt, x(1), x(2).toInt, x(3).toInt, x(4))).toDF()
    }
  }
}
