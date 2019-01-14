package com.cathy

import org.apache.spark.sql.SparkSession

object Setting {
  //setting up kafka properties


  //settling up spark application properties
  private val sparkMaster="local[*]"

  private val sparkDeployMode=" "

  private var sparkAppName=" "

  //setting up working directory path
  private val fileDir="file:///home/cathy/workspace/UserInterestPrediction/events_raw_data/"

  //cassandra properies
  private val cassandraUrl="172.17.0.2"

  //start a sparksession
  def startSpark(name:String):SparkSession={

    sparkAppName=name

    val spark = SparkSession
      .builder
      .master(sparkMaster)
      .appName(sparkAppName)
      .config("spark.cassandra.connection.host", cassandraUrl)
      .getOrCreate()
    //val sc = spark.sparkContext

    spark


  }

  //getting file path
  def getFilePath(filename:String):String={

    fileDir+filename
  }

  def getCassandraTableName(table:String):String={

    table
  }
  def getCassandrakeyspaceName(keyspace:String):String={

    keyspace
  }
  //def connetToCassandra(): Unit ={

  // }

}
