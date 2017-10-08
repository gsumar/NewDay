package com.test.newday.core.tables

import com.test.newday.environment.NewDayContext
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.{FlatSpec, Matchers}
class BasicTableLoaderTest extends FlatSpec with Matchers{

  NewDayContext.init(TestHive)

  "Loading movies test file" should "return 4" in {
    assert(BasicTableLoader.load(
      "src/test/resources/sources/movies.dat", BasicTableType.Movie).count() == 4)
  }

  "Loading ratings test file" should "return 8" in {
    assert(BasicTableLoader.load(
      "src/test/resources/sources/ratings.dat", BasicTableType.Rating).count() == 8)
  }
  "Loading users test file" should "return 5" in {
    assert(BasicTableLoader.load(
      "src/test/resources/sources/users.dat", BasicTableType.User).count() == 5)
  }


}
