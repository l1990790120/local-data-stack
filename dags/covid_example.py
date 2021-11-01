from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

from generic_data_dag import GenericDataDag


class CovidLoadDataDagHistorical(GenericDataDag):

    dag_id = "covid-historical-data-dag"
    dag_description = "jhu covid (1/22-2/29) data dag"
    start_date = datetime(2020, 1, 22, 0, 0)
    end_date = datetime(2020, 2, 29, 0, 0)
    schedule_interval = "@daily"
    catchup = True

    tmp_dir = "tmp"
    out_dir = "out"
    table_name = "csse_covid_19_data_historical"

    analytics_postgres = "postgresql://postgres@postgres:5432/analytics"
    engine = create_engine(analytics_postgres)

    @classmethod
    def download_from_github(cls, *args, **kwargs):
        ds = kwargs["ds"]

        yyyy = ds[0:4]
        mm = ds[5:7]
        dd = ds[8:10]

        url = (
            "https://raw.githubusercontent.com/CSSEGISandData/"
            "COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
            f"{mm}-{dd}-{yyyy}.csv"
        )

        print("download from", url)
        df = pd.read_csv(url)
        df.to_sql(
            cls.table_name, cls.engine, if_exists="append", index=False,
        )

    @classmethod
    def upload_data_to_postgres(cls, *args, **kwargs):
        pass

    @classmethod
    def summarize_dataframe(cls, *args, **kwargs):
        pass

    def get_data(self):
        return PythonOperator(
            task_id="get-data-from-github",
            python_callable=self.download_from_github,
            op_kwargs={"dataset": "ieee-fraud-detection"},
            provide_context=True,
        )

    def upload_data(self):
        return PythonOperator(
            task_id="upload-data",
            python_callable=self.upload_data_to_postgres,
            provide_context=True,
        )

    def summarize_data(self):
        return PythonOperator(
            task_id="summarize-data",
            python_callable=self.summarize_dataframe,
            provide_context=True,
        )


class CovidLoadDataDagNew(CovidLoadDataDagHistorical):
    # new data has different schema

    dag_id = "covid-new-data-dag"
    dag_description = "jhu covid (3/1-3/21) data dag"
    start_date = datetime(2020, 3, 1, 0, 0)
    end_date = datetime(2020, 3, 21, 0, 0)

    table_name = "csse_covid_19_data_new"


class CovidLoadDataDagNewer(CovidLoadDataDagHistorical):
    # new data has different schema

    dag_id = "covid-newer-data-dag"
    dag_description = "jhu covid (3/22+) data dag"
    start_date = datetime(2020, 3, 22, 0, 0)
    end_date = None

    table_name = "csse_covid_19_data_newer"


h = CovidLoadDataDagHistorical()
h_dag = h.get_data_prep_dag()

n = CovidLoadDataDagNew()
n_dag = n.get_data_prep_dag()

nn = CovidLoadDataDagNewer()
nn_dag = nn.get_data_prep_dag()
