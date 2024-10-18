from __future__ import annotations
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    # dag_id: The id of the DAG
    dag_id="f4c7f8a7-4933-4840-8f2f-568526bb9e76",
    # default_args: A dictionary of default parameters to be used as constructor keyword parameters when initialising operators
    default_args={
        "depends_on_past": False,
        "email": ["ehgma0821@kakaopaysec.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=1)
    },
    # description: The description for the DAG to e.g. be shown on the webserver
    description="사용자 프로파일 마케팅플랫폼 제공 데이터 파이프라인",
    # schedule: Defines the rules according to which DAG runs are scheduled
    schedule="* * * * *",
    # start_date: The timestamp from which the scheduler will attempt to backfill
    start_date=datetime(2024, 1, 1),
    # catchup: Perform scheduler catchup (or only run latest)
    catchup=False,
    # tags: List of tags to help filtering DAGs in the UI.
    tags=["dataplatform", "spark", "batch"]
) as dag:
    """
    docs
    - BaseOperator: https://airflow.apache.org/docs/apache-airflow/stable/_modules/airflow/models/baseoperator.html#BaseOperator
    - KubernetesPodOperator: https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/_api/airflow/providers/cncf/kubernetes/operators/pod/index.html#airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator
    """
    k8s_pod_dag = KubernetesPodOperator(
        # task_id: from BaseOperator
        task_id="f4c7f8a7-4933-4840-8f2f-568526bb9e76",
        # name: name of the pod in which the task will run, will be used (plus a random suffix if random_name_suffix is True) to generate a pod id
        name="PROFILE_KUDU_TO_MONGO_ETL",
        # image: Docker image you wish to launch
        image="harbor.kakaopayinvest.com/paysec/pontos/retail-wave-spark-k2m-mkt-prf-an002d04:sandbox-3cd45fa",
        # labels: labels to apply to the Pod
        labels={"foo": "bar"},
        env_vars={
            # "SOURCE_DB_URI": TEMP_SOURCE_DB_URI,
            # "SOURCE_DB_NAME": TEMP_SOURCE_DB_NAME,
            # "SOURCE_TABLE_NAME": TEMP_SOURCE_TABLE_NAME,
            # "TARGET_DB_NAME": TEMP_TARGET_DB_NAME,
            # "TARGET_TABLE_NAME": TEMP_TARGET_TABLE_NAME,
            "SOURCE_CONNECTION_NAME": "dataplatform_kudu",
            "SOURCE_PROPERTIES": {"foo": "bar"},
            "TARGET_CONNECTION_NAME": "mongodb_shuffle",
            "TARGET_PROPERTIES": {"foo": "bar"},
            "SQL": "SELECT * FROM bi.an000d00;",
            # "WRITE_MODE": TEMP_WRITE_MODE,
            # "OPERATION_TYPE": TEMP_OPERATION_TYPE,
        }
    )

    k8s_pod_dag