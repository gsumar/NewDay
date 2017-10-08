package com.test.newday.core.tables


import com.test.newday.constants.TableType
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.{FlatSpec, Matchers}
class LoadTablesTest extends FlatSpec with Matchers{

  "Loading movies test file" should "return 4" in {
    assert(LoadTables.load(TestHive,
      "src/test/resources/sources/movies.dat", TableType.Movie).count() == 4)
  }

  "Loading ratings test file" should "return 8" in {
    assert(LoadTables.load(TestHive,
      "src/test/resources/sources/ratings.dat", TableType.Rating).count() == 8)
  }
  "Loading users test file" should "return 5" in {
    assert(LoadTables.load(TestHive,
      "src/test/resources/sources/users.dat", TableType.User).count() == 5)
  }


}
