# Rate Me

A data engineering project for aggregating movies, grouped by the rating and loading the data frame into the POSTGRES

## Technologies Stack Used

- Postgres
- PySpark
- Apache Airflow


## Install the dependencies
- Installing `pipenv` using `pip install pipenv`
- Install the dependencies using `pipenv sync --dev`
- Load the virtual environment using `pipenv shell`

## Database Setup
- Create the database named `etl_pipeline`
- Create the tables using the below SQL
```sql
create table movies
(
  id          serial primary key,
  name        varchar(255),
  description varchar(255),
  category    varchar(255)
);

create table ratings (
  id serial primary key,
  movie_id integer,
  rating integer,
  foreign key (movie_id) references movies(id) on delete set null
);

```
- Load the `movies.csv` and `ratings.csv` files into the database


## Setting up AirFlow

- Export the following variable
`export AIRFLOW_HOME=~/airflow`

- Initialize the AirFlow database
`airflow db init`

- Use the below command to create an airflow user
```airflow users create \
    --username admin \
    --firstname Ali \
    --lastname Ahmad \
    --role Admin \
    --email example@email.org
```
- Create a password when prompted

- Create a folder under ~/airflow named dags

- Moved the source file to this folder

- Run the Air Flow web UI using `airflow webserver` command

- Run the scheduler using `airflow scheduler` command


## Run the ETL without DAG (Airflow)

- Use the following command in virtual environemnt to run the ETL yourself 
```
python main.py
```