package com.cathy.`trait`

import org.apache.spark.sql.SparkSession

trait Transformer {

//transform dataset into intended layout,format and persist into cassandra
def transform(spark:SparkSession): Unit ={

}



}
