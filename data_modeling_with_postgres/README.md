# Sparkfy  

This project creates a star schema optimized database for queries and analysis on song plays.

## Getting Started

If it's the first time running the script, run the `create_tables.py` to create the database and the tables using:

```
python create_tables.py
```

Once the tables are created, make sure the data sets are present in `data` folder (refer the next session *Data Source*).

Then you may run the `etl.py` as in:

```shell
python etl.py
```

## Data Source

The songs and artists data come from the [Million Song Dataset](http://millionsongdataset.com/), which is a free collection of music metadata. This data set must be in `data/song_data`.

And for testing we have generated fake song play logs using the [event simulator](https://github.com/Interana/eventsim). This data set must be in `data/log_data`.

## Data Model

To allow fast queries we have created a star schema for the song plays. 

The model consists of one fact table called songplays and its dimensions, namely, the song, artist, user and time.

Refer to `sql_queries.py` to check on the table schema. 

### Example of queries


- The number of distinct paid users that played songs by month:

```sql
SELECT month, COUNT(DISTINCT user_id) 
FROM songplays sp 
JOIN time t ON t.start_time = sp.start_time 
WHERE level='paid' 
GROUP BY month 
ORDER BY month;
```

- TOP 5 artists most played:

```sql
SELECT a.name, COUNT(1) AS qtt
FROM songplays sp
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY a.name
ORDER BY qtt DESC
LIMIT 5;
```

- TOP 5 locations that play more songs:


```sql
SELECT sp.location, COUNT(1) AS qtt
FROM songplays sp
GROUP BY sp.location
ORDER BY qtt DESC
LIMIT 5;
```

## ETL 

The ETL for this project has 3 steps:

1. Process songs and artist data from the *Million Song Dataset*
2. Process user data logs from the generated logs
3. Populate the fact table and the dimensions doing upserts or ignoring in case of conflicts

These steps are executed by the etl.py script, which calls the appropriate function for each of the json file in the `data` folder. 

The create_tables.py script recreates the database and tables used by the script. Be aware!!! The main function will delete all persisted data!

The sql_queries.py contains the SQL for creating tables, insert records and for selecting.

For testing the steps executed by the script we have to notebooks: etl.ipynb and test.ipynb.
