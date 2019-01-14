package com.cathy

import com.cathy.`trait`.Transformer
import org.apache.spark.sql.SparkSession

class Transforming(spark:SparkSession) {self: Transformer=>

  def start()=transform(spark)


}
