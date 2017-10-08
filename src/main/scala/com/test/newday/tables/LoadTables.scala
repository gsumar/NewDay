package com.test.newday.tables

import com.test.newday.constants.TableType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object LoadTables {


  val fieldsTerminatedBy = "::"

  case class Movie (movieId:Int, title:String, genre:String)
  case class Rating (userId:Int, movieId:Int, rating:Int, timestamp:Int)
  case class User (userId:Int, genge:String, age:Int, Occupation:Int, zipCode:String)

  def load(sqlContext:HiveContext, inputPath: String, table:TableType.EnumVal): DataFrame = {
    import sqlContext.implicits._
    val rdd = sqlContext.sparkContext.textFile(inputPath)

    table match  {
      case TableType.Movie => rdd.map(x => x.split(fieldsTerminatedBy)).map(x => Movie(x(0).toInt, x(1), x(2))).toDF()
      case TableType.Rating => rdd.map(x => x.split(fieldsTerminatedBy)).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt)).toDF()
      case TableType.User => rdd.map(x => x.split(fieldsTerminatedBy)).map(x => User(x(0).toInt, x(1), x(2).toInt, x(3).toInt, x(4))).toDF()
    }
  }
}
