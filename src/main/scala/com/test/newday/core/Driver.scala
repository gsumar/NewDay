package com.test.newday.core

import com.test.newday.environment.NewDayContext

object Driver {
  def main(args: Array[String]) {
    NewDayContext.init()
    new ProblemResolver(args).run()
  }
}