package com.cathy.`trait`

import com.cathy.Setting
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

trait EventAttendeesTransformer extends Transformer {


  private var eventAttendeesRawRDD:RDD[String]=_

  private var eventAttendeesDF:DataFrame=_




  override def transform(spark:SparkSession): Unit = {

    ReadEvntAttndeesRaw(spark)

    saveEvntAttndeesRaw(spark)

    //clean and transform evntAttndeesRawRDD  to evntAttndeesDF  and save into Cassandra as table event_attendees
    createEvntAttndees(spark)

    //transform evntAttndeesDF to evntAttndeesCntDF and save into cassandra as table event_attendees_count
    createEvntAttndeesCnt(spark)

    createUserAttndSummary(spark)

    creatUserAttndCnt(spark)

  }


  //read data from data source and get eventAttendeesRawRDD for later use
  def ReadEvntAttndeesRaw(spark:SparkSession):Unit={

    //ingesting from local file system ,will be modified to extract data from kafka
    val sc = spark.sparkContext

    eventAttendeesRawRDD = sc.textFile(Setting.getFilePath("event_attendees_raw.csv"))

  }

  // save raw data into hdfs
  def saveEvntAttndeesRaw(spark:SparkSession): Unit ={

    //save into localfile will be modified to hdfs

    eventAttendeesRawRDD.saveAsTextFile(Setting.getFilePath(("hdfssave/event_attendee_raw")))

  }

  // save table event_attendees to cassandra ,assign value to eventAttendeesDF and register a table as eventAttendeeTable.
  def createEvntAttndees(spark: SparkSession): Unit ={

    //transform event_attendees_raw.csv from (eventsid,four groups of userid whose attend_type is yes, maybe, invited, no respectively) to  (id ,eventid, userid, attend_type)
    val seq = Array(" ", "yes", "maybe", "invited", "no")

    val eventAttndeesRDD=eventAttendeesRawRDD.mapPartitionsWithIndex((iter, x) => if (iter == 0) x.drop(1) else x)
      .flatMap(x => for (i <- 1 to (x.split(",").length - 1)) yield {(x.split(",")(0), x.split(",")(i), seq(i))})
      .flatMap(x => x._2.split(" ").map(y => (x._1, y, x._3))).map(x=>(x._1.toString+x._2.toString+x._3,x._1,x._2,x._3))

    //remove rows with missing values,the number of rows where userid equals null is 2082, the number of rows where status equals null is 0. the # of duplicated row is 2
    // eventAttendUserWithoutNullDF.count()=11245008
    import spark.implicits._

    eventAttendeesDF=eventAttndeesRDD.toDF("id","eventid","userid","attend_type").na.drop().dropDuplicates().where('id=!=" ")

    //register as a table
    eventAttendeesDF.createOrReplaceTempView("eventAttendeeTable")

    //write to cassandra # fo record in cassandra is 11247090
    eventAttendeesDF.write.format("org.apache.spark.sql.cassandra").option("table",Setting.getCassandraTableName("event_attendees")).option("keyspace",Setting.getCassandrakeyspaceName("events")).mode(org.apache.spark.sql.SaveMode.Append).save()

  }

  //save table event_attendee_count to cassandra, get from query out of eventAttendeeTable
  def createEvntAttndeesCnt(spark:SparkSession)={


    val eventAttendeeCountDF=eventAttendeesDF.groupBy("eventid","attend_type").count.withColumn("id",monotonically_increasing_id)

    //write into Cassandra as table event_attendee_count #=85026
    eventAttendeeCountDF.write.format("org.apache.spark.sql.cassandra").option("table",Setting.getCassandraTableName("event_attendee_count")).option("keyspace",Setting.getCassandrakeyspaceName("events")).mode(org.apache.spark.sql.SaveMode.Append).save()




  }

  // save table user_attend_summary to cassandra and register a table as user_attend_summary_table,get from query out of eventAttendeeTable

  def createUserAttndSummary(spark:SparkSession): Unit ={

    val userAttendSummaryDF=spark.sql("with event_user_attend_journal as (select eventid,userid, CASE WHEN attend_type = 'invited' THEN 1 ELSE 0 END AS invited," +
            "CASE WHEN attend_type = 'yes' THEN 1 ELSE 0 END AS attended,CASE WHEN attend_type = 'no' THEN 1 ELSE 0 END AS not_attended,CASE " +
            "WHEN attend_type = 'maybe' THEN 1 ELSE 0 END AS maybe_attended from eventAttendeeTable) select eventid,userid,max(invited)as invited," +
            "max(attended) as attended, max(not_attended) as not_attend, max(maybe_attended) as maybe_attend from event_user_attend_journal group by eventid,userid")

    //register a table
    userAttendSummaryDF.createOrReplaceTempView("user_attend_summary_table")

    //write into Cassandra as table  user_attend_summary
    userAttendSummaryDF.withColumn("id",monotonically_increasing_id).write.format("org.apache.spark.sql.cassandra").option("table",Setting.getCassandraTableName("user_attend_summary")).option("keyspace",Setting.getCassandrakeyspaceName("events")).mode(org.apache.spark.sql.SaveMode.Append).save()

    //invited=9418761 yes 831686, no 474438  , maybe= 522205
    // df=11246276 cassandra=11246276

  }

 //save table user_attend_count to cassandra,
  def creatUserAttndCnt(spark:SparkSession): Unit ={

   val creatUserAttndCntDF=spark.sql("select userid,SUM(invited) AS invited_count,SUM(attended) AS attended_count," +
     "SUM(not_attend) AS not_attend_count,SUM(maybe_attend) AS maybe_attend_count from user_attend_summary_table group by userid")

    //write into Cassandra as table  user_attend_summary
    creatUserAttndCntDF.filter(col("userid")=!="").write.format("org.apache.spark.sql.cassandra").option("table",Setting.getCassandraTableName("user_attend_count")).option("keyspace",Setting.getCassandrakeyspaceName("events")).mode(org.apache.spark.sql.SaveMode.Append).save()


  }


}
