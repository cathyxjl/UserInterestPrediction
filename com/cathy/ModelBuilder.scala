package com.cathy

import com.cathy.Setting._

object ModelBuilder {


  def main(args: Array[String]): Unit = {

    // create a sparksession and sparkcontext
    val spark=startSpark("ModelBuilder")

    //extract features
    val features=new featureExtractor(spark).getFeatures()

    //train data to ge a prediction model

  }
}
