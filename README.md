# UserInterestPrediction

Build prediction model to predict what events users will be interested in based on user actions, event metadata, and demographic information

## Developing Enviroment

  hortonwork sandbox:Kafka,Spark,HDFS/
  cassandra container/
  Nifi container/
  intellij with scala plug-in
  
## Project Details

There are six csv files will be collected through NIFI and ingested into KAFKA

Each data set will be CLEANED AND TRANSFORMED into several sub dataset and saved into Cassandra

load transformed data from cassandra into SPARK SQL to extract features

create machine learing pip-line to build model

predict test data and evaluate the model

## What I accomplished 

Compose the project

Cleaned and Transformed event_attendees_raw into 
event_attendees,event_attendee_count,user_attend_summary,user_attend_count,and save into cassandra.

I am still working on other components

    
