package com.test.newday.core.output

import org.apache.spark.sql.DataFrame

object SaveAsParquet {
  def save(df:DataFrame, path:String) = {
    df.write.parquet(path)
  }
}
