import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

from generic_data_dag import GenericDataDag


class WikipediaDataDagHistorical(GenericDataDag):
    """
    This is a sample dag for pulling wikipedia pageviews data on an hourly
    basis.
    """

    dag_id = "wikipedia-historical-data-dag"
    dag_description = "Wikipedia historical pageviews data dag"
    # TODO: update this to be the start date
    start_date = datetime.now() - timedelta(days=1, hours=5)
    end_date = datetime.now() - timedelta(days=1)
    schedule_interval = "@hourly"
    catchup = True

    out_dir = "out"
    table_name = "wikipedia_pageviews_data_historical"

    analytics_postgres = "postgresql://postgres@postgres:5432/analytics"
    engine = create_engine(analytics_postgres)

    @classmethod
    def download_from_wikipedia(cls, *args, **kwargs):
        ts = kwargs["ts"]

        yyyy = ts[0:4]
        mm = ts[5:7]
        dd = ts[8:10]
        hh = ts[11:13]

        # url format:
        # https://dumps.wikimedia.org/other/pageviews/2021/2021-10/pageviews-20211001-010000.gz
        url = (
            f"https://dumps.wikimedia.org/other/pageviews/{yyyy}/{yyyy}-{mm}/"
            f"pageviews-{yyyy}{mm}{dd}-{hh}0000.gz"
        )
        print("download from", url)
        df = pd.read_csv(
            url,
            sep=" ",
            header=None,
            names=[
                "domain_code",
                "page_title",
                "count_views",
                "total_response_size",
            ],
        )
        df["dt"] = ts

        df.to_csv(
            os.path.join(cls.out_dir, f"pageviews-{yyyy}{mm}{dd}-{hh}.csv"),
            index=False,
        )

    @classmethod
    def upload_data_to_s3(cls, *args, **kwargs):
        pass

    @classmethod
    def summarize_dataframe(cls, *args, **kwargs):
        pass

    def get_data(self):
        return PythonOperator(
            task_id="get-data-from-wikipedia",
            python_callable=self.download_from_wikipedia,
            provide_context=True,
        )

    def upload_data(self):
        return PythonOperator(
            task_id="upload-data",
            python_callable=self.upload_data_to_s3,
            provide_context=True,
        )

    def summarize_data(self):
        return PythonOperator(
            task_id="summarize-data",
            python_callable=self.summarize_dataframe,
            provide_context=True,
        )


w = WikipediaDataDagHistorical()
w_dag = w.get_data_prep_dag()
