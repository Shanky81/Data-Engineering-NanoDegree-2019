Project Name: Sparkify Song-Play Data Analysis Using RDBMS

1. Summary Of The Project
2. Objective
3. Technical Requirement
4. Pre-requisite
5. Repository File Details 
6. How To Run Python Scripts


1. Summary Of The Project

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Sparkify - Postgres database (RDBMS) was used to store songs and useractivity data to further analyze and create some data related facts based on understanding. 
After going through feed files (song data and log data), Start schema was adopted and dimession (users, songs, artists, time) and fact (songplays) tables were created and designed to optimize queries on song play analysis. 

    songplays-Fact Table keys/references:-    
                start_time REFERENCES time(start_time).
                user_id  REFERENCES users(user_id).
                song_id  REFERENCES songs(song_id).
                artist_id REFERENCES artists(artist_id).
    
An ETL pipeline was developed using python to pull latest data into Sparkify from files present in two local directories for continuous analysis. 

Test were performed to validate ETL pipeline by running queries given by the analytics team from Sparkify database.

2. Objective
Analysis on creating understanding on which songs users are listening to.

3. Technical Requirement
To create a Postgres database with tables designed to optimize queries on song play analysis by creating database - Star schema and ETL pipeline for further analysis.


4. Pre-requisite
Python and PostgreSQL up and running instance.

5. Repository File Details

    Data Source:
    
            a. /data/song_data : All files present in this directory are in JASON format and contains song related meta data like song names and artist. 
            b. /data/log_data : All files present in this directory are log files generated from music treaming application in JASON format and contains log related meta data  like users who listen songs and listen start and duration of time.


    Technical Files: 
    
            a. sql_queries.py contains all sql queries used in this project.
            b. create_tables.py to create your database and tables.
            c. test.ipynb to confirm the creation of your tables with the correct columns.
            d. etl.ipynb reads and processes only one file from song_data and log_data and loads the data into your tables. 
            This notebook contains detailed instructions on the ETL process for each of the tables.
            e. Using etl.ipynb, etl.py developed, which reads and processes all files from song_data and log_data and loads them into your tables.
            
    Informative Files: 
    
            a. README.md file that includes all required details to create understanding about this project.
            

6. How To Run Python Scripts

Steps as per below: 

            a. Run create_tables.py to create your database and tables using sql_queries.py.
            b. Run test.ipynb to confirm the creation of your tables with the correct columns. 
            Make sure to click "Restart kernel" to close the connection to the database after running this notebook.
            c. Use etl.ipynb to create understanding on etl.py file, where line by line execution can be done (sort of debug).
            d. run etl.py to process the entire datasets. 
            e. when starting from begining always make sure to click "Restart kernel" to close the connection to the database 
            after running this notebook.
            
-------------------------------------------------------------------------------------------------------------------------------

Sample Analytical Queries:

1. Top 10 user_id's with used session id:
Select session_id, user_id, Count(*) as Total_Records from songplays group by session_id, user_id order by Total_Records desc  LIMIT 10;


2. Top 10 Session and their Location:
Select location, session_id, Count(*) as Total_Records from songplays group by location, session_id order by Total_Records desc  LIMIT 10;








------------------------------------------------------------------------------------------------------------------------