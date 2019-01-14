package com.cathy.`trait`

import org.apache.spark.sql.SparkSession

trait UserFriendsTransformer extends Transformer {

  override def transform(spark: SparkSession): Unit = {




  }

  //read data from data source
  def readUserFriendsRaw(spark:SparkSession): Unit ={

  }

  //persist raw data into Hdfs
  def saveUserFriendsRaw(spark:SparkSession):Unit={


  }

  def createUserFriend(spark:SparkSession)={



  }

  def createUserFriendCnt

}
