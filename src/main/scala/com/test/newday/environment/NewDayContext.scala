package com.test.newday.environment

import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object NewDayContext {
  var conf: SparkConf = _
  var sc: SparkContext = _
  var sqlContext: HiveContext = _

  def init() = {
    var properties : Properties = null

    val url = getClass.getResource("/ContextConfiguration.properties")
    if (url != null) {
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
    }

    val sparkAppName = "sparkAppName"
    val master = "master"

    conf = new SparkConf().setAppName(properties.getProperty(sparkAppName))
    conf.contains("spark.master") match {
      case false => conf.setMaster(properties.getProperty(master))
      case _ =>
    }
    sc = new SparkContext(conf)
    sqlContext = new HiveContext(sc)
  }
}
