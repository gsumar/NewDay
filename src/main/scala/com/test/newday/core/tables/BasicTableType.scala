package com.test.newday.core.tables

object BasicTableType {
  sealed trait EnumVal
  case object Movie extends EnumVal
  case object Rating extends EnumVal
  case object User extends EnumVal
  val BasicTableTypes = Seq(Movie, Rating, User)
}
