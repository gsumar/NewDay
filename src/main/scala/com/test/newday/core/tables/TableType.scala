package com.test.newday.core.tables

object TableType {
  sealed trait EnumVal
  case object Movie extends EnumVal
  case object Rating extends EnumVal
  case object User extends EnumVal
  val TableTypes = Seq(Movie, Rating, User)
}
