import uuid
from loguru import logger
from urllib.parse import urlencode
from apps.common.helper import call_api_with_retries, get_secret_from_vault

def trigger_dag(
    dag_id: str,
    logical_date: str,
    airflow_domain: str,
    env: str,
    airflow_manual_trigger_timeout: int,
    airflow_manual_trigger_max_retries: int,
    airflow_manual_trigger_sleep_time: int,
    logger=logger
):
    url = f"https://{airflow_domain}.kakaopayinvest.com/api/v1/dags/{dag_id}/dagRuns?"
    payload = {
        "limit": 1,
        "execution_date_gte": logical_date,
        "execution_date_lte": logical_date,
    }
    payload = urlencode(payload)
    url += payload
    logger.info(f"[{env}] URL: {url}")
    response = call_api_with_retries(
        method="GET",
        url=url,
        data={},
        timout=airflow_manual_trigger_timeout,
        max_retries=airflow_manual_trigger_max_retries,
        sleep_time=airflow_manual_trigger_sleep_time,
        auth=("username", "password"),
        logger=logger
    )

    logger.info(f"[{env}] 응답: {response}")

    if len(response["dag_runs"]) > 0:
        dag_run_id = response["dag_runs"][0]["dag_run_id"]
        logger.info(f"[{env}] {dag_run_id} 가 존재하여, clear 처리로 재시작합니다")
        payload = {
            "dag_run_id": dag_run_id,
        }
        dag_run_id = urlencode(payload).split("=")[1]
        url = f"https://{airflow_domain}.kakaopayinvest.com/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/clear"
        params = {
            "dry_run": False
        }
        logger.info(f"[{env}] URL: {url}")

        response = call_api_with_retries(
            method="POST",
            url=url,
            data=params,
            timout=airflow_manual_trigger_timeout,
            max_retries=airflow_manual_trigger_max_retries,
            sleep_time=airflow_manual_trigger_sleep_time,
            auth=("username", "password"),
            logger=logger
        )
        logger.info(f"[{env}] 응답: {response}")
        logger.info(f"[{env}] 페이로드: {params}")
    else:
        logger.info(f"[{env}] 신규 dag 실행을 요청합니다")
        url = f"https://{airflow_domain}.kakaopayinvest.com/api/v1/dags/{dag_id}/dagRuns"
        params = {
            "logical_date": logical_date,
            "dag_run_id": str(uuid.uuid4())
        }
        logger.info(f"[{env}] URL: {url}")

        response = call_api_with_retries(
            method="POST",
            url=url,
            data=params,
            timout=airflow_manual_trigger_timeout,
            max_retries=airflow_manual_trigger_max_retries,
            sleep_time=airflow_manual_trigger_sleep_time,
            auth=("username", "password"),
            logger=logger
        )
        logger.info(f"[{env}] 응답: {response}")
        logger.info(f"[{env}] 페이로드: {params}")