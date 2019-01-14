package com.cathy

import com.cathy.`trait`.EventAttendeesTransformer
import org.apache.spark.sql.SparkSession

class featureExtractor(spark:SparkSession) {

  //get feature table for machine learning
  def getFeatures(): Unit ={

   //clean data and transform data into intended layout ,then persist into Cassandra
    transform()

    //extract Features ,return a feature vector and save feature data set into Cassandra
    extractFeatures()
  }



  //clean the data and transform data into intended layouts and formats
  def transform(): Unit ={

    //clean and transform dataset of event_attendees_raw  to several datasets (event_attendees, user_attend_status,user_attend_event_count)
    (new Transforming (spark) with EventAttendeesTransformer).start()

  }


  //extracting freatures by a list of dataset transformed  by method transform()
  def extractFeatures(){


  }

}
