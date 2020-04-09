from abc import abstractmethod
from datetime import datetime, timedelta

from airflow import DAG


class GenericDataDag:
    """This is meant to serve as shared
    """

    default_args = {
        "owner": "lulu",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(seconds=5),
    }

    dag_id = "generic-example-dag"
    dag_description = "a generic example dag"
    start_date = None
    end_date = None
    catchup = False
    schedule_interval = None

    @property
    def dag_args(self):
        return {
            "dag_id": self.dag_id,
            "description": self.dag_description,
            "schedule_interval": self.schedule_interval,
            "is_paused_upon_creation": False,
            "catchup": self.catchup,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

    def get_data_prep_dag(self):
        dag = DAG(**self.dag_args, default_args=self.default_args)
        with dag:
            get_data = self.get_data()
            upload_data = self.upload_data()
            summarize_data = self.summarize_data()

        get_data >> upload_data >> summarize_data
        return dag

    def get_model_dag(self):
        dag = DAG(**self.dag_args, default_args=self.default_args)
        with dag:
            get_data = self.get_data()
            split = self.split_train_and_test()
            train = self.train()
            validate = self.validate()

        get_data >> split >> train >> validate
        return dag

    @abstractmethod
    def get_data(self):
        """download data to local
        """
        raise NotImplementedError

    @abstractmethod
    def upload_data(self):
        """upload data to db
        """
        raise NotImplementedError

    @abstractmethod
    def summarize_data(self):
        """upload data to db
        """
        raise NotImplementedError

    @abstractmethod
    def split_train_and_test(self):
        """[summary]
        """
        raise NotImplementedError

    @abstractmethod
    def train(self):
        """[summary]
        """
        raise NotImplementedError

    @abstractmethod
    def validate(self):
        """[summary]
        """
        raise NotImplementedError
