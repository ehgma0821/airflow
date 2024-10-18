import hvac, requests, time
from loguru import logger

def call_api_with_retries(
    method: str,
    url: str,
    data: dict,
    timeout: float,
    max_retries: int,
    sleep_time: float,
    auth: tuple,
    logger=logger,
    headers: dict = {
        "Content-type": "application/json",
        "Accept": "application/json"
    }
) -> dict:
    num_retries = 0
    error_flag = True
    while error_flag and num_retries < max_retries:
        try:
            rsps = dict()
            if method == "POST":
                response = requests.post(
                    url=url,
                    json=data,
                    timeout=timeout,
                    headers=headers,
                    auth=auth
                )
            elif method == "GET":
                response = requests.get(
                    url=url,
                    timeout=timeout,
                    headers=headers,
                    auth=auth
                )
            response_json = response.json()
            logger.info(response_json)
            response.raise_for_status()
            rsps = response_json
            error_flag = False
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
        ) as err:
            num_retries += 1
            logger.info(f"Failed ({num_retries} times) : Trying to call {url} again")
            e = {"error_code": type(err).__name__, "error_message": err.__str__()}
            logger.info(e)
            time.sleep(sleep_time)
        except Exception as unexpected_err:
            logger.error(f"예기치 못한 에러가 발생했습니다: {type(unexpected_err).__name__}, {str(unexpected_err)}")
            raise
        
        if num_retries >= max_retries:
            raise Exception(f"API 호출 재시도 최대 횟수 ({max_retries}) 를 초과했습니다.")
    
    return rsps


def get_secret_from_vault(url, path, token):
    client = hvac.Client(url=url, token=token)
    read_response = client.secrets.kv.read_secret_version(path=path, mount_point="kv")

    return dict(read_response["data"]["data"])