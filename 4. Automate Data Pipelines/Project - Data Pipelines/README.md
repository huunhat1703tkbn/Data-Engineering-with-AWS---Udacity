
# Project 4: Data Pipelines with Apache Airflow

## Quick start

#### This project handles music app Sparkify, trying to introduce more automation to their ETL pipelines, with the tool Apache Airflow.

## Database

####  Sparkify contains two datasets with the S3 links:
- s3://udacity-dend/song_data/
- s3://udacity-dend/log_data/

### Resources
- Visual Studio Code.
- Airflow.

## DAG

#### start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> \
#### [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
#### run_quality_checks >> end_operator

### Scripts
- Operators: data_quality.py, load_dimension.py, load_fact.py, stage_redshift.py.
- Helpers: sql_queries.py.
- DAG: udac_example_dag.py.
- Table creator: create_tables.sql.