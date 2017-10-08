package com.test.newday.core

import com.test.newday.constants.NewDayContext

object Driver {
  def main(args: Array[String]) {
    NewDayContext.init()
    new ProblemResolver(args, NewDayContext.sqlContext).run()
  }
}