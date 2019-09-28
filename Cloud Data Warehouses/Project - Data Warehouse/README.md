Introduction
    A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

   Task is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to and should give analytical insights on data.


Technical Description
 To complete the business requirement, we will load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables for further analysis.
 


Project Datasets
We will be working with two datasets that reside in S3. 
Here are the S3 links for each where source data is provided:
1. Song data: s3://udacity-dend/song_data
2. Log data: s3://udacity-dend/log_data
3. Log data json path: s3://udacity-dend/log_json_path.json


Schema for Song Play Analysis
    Using the song and event datasets, you'll need to create a star schema        optimized for queries on song play analysis. This includes the following tables.

    Fact Table
    1. songplays - records in event data associated with song plays i.e. records        with page NextSong

    Required columns as follows:
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id,            location, user_agent

    Dimension Tables
    1. users - users in the app

    Required columns as follows:
    user_id, first_name, last_name, gender, level

    2. songs - songs in music database

    Required columns as follows:
    song_id, title, artist_id, year, duration

    3. artists - artists in music database

    Required columns as follows:
    artist_id, name, location, lattitude, longitude

    4. time - timestamps of records in songplays broken down into specific units

    Required columns as follows:
    start_time, hour, day, week, month, year, weekday



Create Table Schemas
    
    1. Design schemas for your fact and dimension tables.
    
    2. Write a SQL CREATE statement for each of these tables in sql_queries.py
    Complete the logic in create_tables.py to connect to the database and create these tables.
    
    3. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
    
    4. Launch a redshift cluster and create an IAM role that has read access to S3.
    5. Add redshift database and IAM role info to dwh.cfg.
    6. Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console       for this.

Build ETL Pipeline
    1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
    2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
    3. Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the       expected results.
    4. Delete your redshift cluster when finished.

Steps Carried out in Project:

1. Import all libraries required AWS s3, postgres and etl related activities.
2. get the AWS access keys from the configuration of AWS Cluster, store them in      parameter config file
3. Using the S3 bucket, we can check whether log files and song data files are      present or not.
4. Create an IAM User Role, assign read only permissions and create the Redshift    Cluster
5. Get the Value of endpoint (Host Name) and Role into configuration file. 
6. Authorize the security access group to Default TCP/IP Address
7. Launch database connectivity using confign file.
8. Go to Terminal write the command/ "python create_tables.py" and then "etl.py"
   or 
8. % RUN create_tables.py" and then "etl.py" in jupiter note book
9. It should take around 2-5 minutes.
10. Once ran, confirm that data is present in the tables and then take records count for all tables.
11. Once results are matching and project is successful, delete the cluster, roles and assigned permission.