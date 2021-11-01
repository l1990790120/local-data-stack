# Local Data Stack

This is an simple data analytics or machine learning stack that runs on your
local machine.

The `docker-compose` is consisted of:

1. [Airflow](https://airflow.apache.org/)
2. [Superset](https://superset.apache.org/)
3. [Postgres](https://www.postgresql.org/)

The `dags` directory has some nice examples to show you how you can use this 
repo to bootstrap data analytics or machine learning projects quickly on your
local machine.

## Quick Start

**Reminders:** You'll need `docker` and `docker-compose`.

Simply clone the repo, `cd` into repo directory and run `docker-compose up -d`

```bash
git clone git@github.com:l1990790120/local-data-stack.git
cd local-data-stack
docker-compose up -d
```

Suppose things are working as expected, you should see

- Airflow: [http://localhost:8080/](http://localhost:8080/)
- Superset: [http://localhost:8088/](http://localhost:8088/)

**Note:**

1. Default login for airflow is airflow:airflow.
2. For superset, you will have to initialize database for the first time, details [here](https://github.com/amancevice/docker-superset#database-initialization).

```bash
docker exec -it local-data-stack_superset_1 superset-init
```

## Technical Details

The Postgres is used as backend for both Airflow (under user airflow) and Superset (under user superset). Data are loaded into database `analytics` (under user postgres).

In Superset, you can add database with sql connection string: `postgresql://postgres@postgres:5432/analytics`.

Or, if you just want to run some sql queries, exec into the docker container

```bash
docker exec -it local-data-stack_postgres_1 bash
```

Within Postgres container, run

```bash
psql -U postgres -d analytics
```

And you'll see all the data you've loaded with Airflow.

## Example Dags

I have put some interesting data pipeline `dags` directory. I'll continue to add more as I come across. Feel free to contribute as well.

- [JHU Covid Data](https://github.com/l1990790120/local-data-stack/blob/master/dags/covid_example.py)
- More to come
