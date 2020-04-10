import os
from datetime import datetime
from datetime import timedelta
from zipfile import ZipFile

import pandas as pd
from airflow.operators.python_operator import PythonOperator
from kaggle import api as kapi
from sqlalchemy import create_engine
from airflow import DAG

from generic_data_dag import GenericDataDag


class KaggleLoadDigitsDataDag(GenericDataDag):
    """
    Example to read kaggle digits data from:
    https://www.kaggle.com/c/digit-recognizer

    Follow kaggle docs to get api token here and place kaggle.json in repo directory:
    https://www.kaggle.com/docs/api

    Args:
        GenericDataDag ([type]): [description]

    Returns:
        [type]: [description]
    """

    dag_id = "kaggle-digits-data-dag"
    dag_description = (
        "kaggle digits data: https://www.kaggle.com/c/digit-recognizer"
    )
    start_date = datetime.now() - timedelta(days=1)
    end_date = None
    schedule_interval = "@once"
    catchup = False

    tmp_dir = "tmp/kaggle_digits"
    out_dir = "out/kaggle_digits"
    table_name = "kaggle_digits"
    kaggle_dataset = "digit-recognizer"

    analytics_postgres = "postgresql://postgres@postgres:5432/analytics"
    engine = create_engine(analytics_postgres)

    @classmethod
    def get_kaggle_digits_data(cls, *args, **kwargs):
        to_postgres = kwargs.get("to_postgres", False)
        kapi.authenticate()
        kapi.competition_download_files(cls.kaggle_dataset, path=cls.tmp_dir)

        os.makedirs(cls.tmp_dir, exist_ok=True)
        os.makedirs(cls.out_dir, exist_ok=True)

        with ZipFile(
            os.path.join(cls.tmp_dir, f"{cls.kaggle_dataset}.zip"), "r"
        ) as f:
            f.extractall(cls.out_dir)

        for f in os.listdir(cls.out_dir):
            print("loading", f)
            file_path = os.path.join(cls.out_dir, f)
            table_name = f"{cls.table_name}_{os.path.splitext(f)[0]}"

            df = pd.read_csv(file_path)
            df.head(0).to_sql(
                table_name, cls.engine, if_exists="replace", index=False,
            )

            # NOTE: this is an example how you could load it into postgres
            # we probably don't need it though
            if to_postgres:
                conn = cls.engine.raw_connection()
                with conn.cursor() as cursor:
                    with open(file_path, "r") as tb:
                        cursor.copy_expert(
                            f"""COPY {table_name} FROM STDIN """
                            """WITH CSV HEADER DELIMITER as ','""",
                            file=tb,
                        )
                conn.commit()
                conn.close()

    @classmethod
    def split_train_test(self, *args, **kwargs):
        pass

    @classmethod
    def train_model(self, *args, **kwargs):
        pass

    @classmethod
    def validate_model(self, *args, **kwargs):
        pass

    def get_data(self):
        return PythonOperator(
            task_id="get-kaggle-data",
            provide_context=True,
            python_callable=self.get_kaggle_digits_data,
            op_kwargs={"to_postgres": False}
        )

    def split_train_and_test(self):
        return PythonOperator(
            task_id="split-train-test",
            python_callable=self.split_train_test,
            provide_context=True,
        )

    def train(self):
        return PythonOperator(
            task_id="train-model",
            python_callable=self.train_model,
            provide_context=True,
        )

    def validate(self):
        return PythonOperator(
            task_id="validate-model",
            python_callable=self.validate_model,
            provide_context=True,
        )


k = KaggleLoadDigitsDataDag()
k_dag = k.get_model_dag()
