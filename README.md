# Open Food Facts exercise

## Design Diagrams

### Airflow pipelines
![image](https://github.com/talgalfsky/open_food_facts/blob/main/images/open_food_data_elt_with_airflow.jpeg)

This approach uses an `initial` pipeline to load the bulk data into a DataBase. After the initial load is completed. Only the `incremental` pipeline needs to run at a scheduled interval. The scheduling is orchestrated by Apache Airflow. Each task runs in a Kubernetes container which can be configured to support any platform (Google, AWS, other...).

### SQL transforms
![SQL Transforms](https://github.com/talgalfsky/open_food_facts/blob/master/images/open_food_data_elt_with_airflow.jpeg?raw=true)

### Bulk and Stream
Initial ingest can is done in bulk from a [CSV file](https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv) containing the entire DB. The CSV file is updated daily.

For the incremental load, updates of data at 15 minutes interval can be obtained by making an API call requesting the newest data on the database:
Example GET call:

``` bash
https://world.openfoodfacts.org/cgi/search.pl?json=true&action=process&sort_by=last_modified_t&page_size=100
```

This call will capture the last 100 changes made to the data sorted by descending order from newest to oldest.


### Option 2: GCP based solution
When going with a platform specific solution, for Google Cloud Platform, a pipeline can be designed using DataFlow to handle bulk vs. stream operations.
![GCP Pipeline](https://github.com/talgalfsky/open_food_facts/blob/master/images/open_food_data_elt_with_airflow.jpeg?raw=true)
For the specific case of Open Food 

## Dashboard and EDA SQL
While working on the task, I loaded the bulk CSV file to GS Bucket and then ingested it to a table in BigQuery.
During Exploratory Data Analysis (EDA) I used the SQL queries in `SQL\eda.sql` to clean up the data and create some auxiliary tables.

This [Data Studio Dashboard] (https://datastudio.google.com/s/uWBGaQNs5iY) is intended to answer the questions in the exercise
