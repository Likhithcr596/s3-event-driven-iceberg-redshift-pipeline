import json
import logging
import sys
import boto3
import configparser
import io
import urllib.request
import urllib.parse
import urllib.error
from html.parser import HTMLParser

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Dummy S3 folder names mapped to dummy Airflow DAG IDs.
# Replace these with authorized values in your actual environment.
object_key_and_dag_name = {
    "demo_campaign_a_attributes/": "demo_dag_a",
    "demo_campaign_a_mapping_full/": "demo_dag_a",
    "demo_campaign_a_mapping_resolved/": "demo_dag_a",
    "demo_campaign_b_attributes/": "demo_dag_b",
    "demo_campaign_b_mapping_full/": "demo_dag_b",
    "demo_campaign_b_mapping_resolved/": "demo_dag_b",
}


def get_ssm_params():
    """
    Fetch credentials and configuration from AWS SSM Parameter Store.

    Expected parameter value format:

    [demo-airflow-trigger]
    airflow_url = https://airflow.example.com
    username = demo_user
    password = demo_password
    """
    client = boto3.client("ssm")

    response = client.get_parameter(
        Name="/demo/service",
        WithDecryption=True
    )

    param_buf = io.StringIO(response["Parameter"]["Value"])
    parser = configparser.ConfigParser()
    parser.read_file(param_buf)

    config = {}
    for section in parser.sections():
        config[section] = dict(parser.items(section))

    return config


# Load credentials securely from SSM.
ssm_config = get_ssm_params()
creds = ssm_config.get("demo-airflow-trigger", {})

AIRFLOW_URL = creds.get("airflow_url", "").strip("\"'")
username = creds.get("username", "").strip("\"'")
password = creds.get("password", "").strip("\"'")


class CSRFParser(HTMLParser):
    """Extract the CSRF token from the login page."""

    def __init__(self):
        super().__init__()
        self.csrf_token = None

    def handle_starttag(self, tag, attrs):
        if tag == "input":
            attr_dict = dict(attrs)
            if attr_dict.get("id") == "csrf_token":
                self.csrf_token = attr_dict.get("value")


def get_csrf_token(opener):
    url = f"{AIRFLOW_URL}/login/"

    with opener.open(url) as resp:
        html = resp.read().decode("utf-8")

    parser = CSRFParser()
    parser.feed(html)

    return parser.csrf_token


def is_dag_running(opener, dag_id):
    url = (
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
        "?order_by=-execution_date&limit=5"
    )

    with opener.open(url) as resp:
        if resp.status != 200:
            logger.error("Failed to fetch DAG runs")
            return False

        data = json.loads(resp.read().decode("utf-8"))

    dag_runs = data.get("dag_runs", [])

    for dag_run in dag_runs:
        state = dag_run.get("state", "").lower()

        if state in ["queued", "running"]:
            logger.warning(
                "DAG is currently %s. Not triggering.", state
            )
            return True

    return False


def login_and_trigger_dag(dag_id, conf=None):
    try:
        cookie_processor = urllib.request.HTTPCookieProcessor()
        opener = urllib.request.build_opener(cookie_processor)

        # Step 1: Retrieve CSRF token.
        csrf_token = get_csrf_token(opener)

        if not csrf_token:
            logger.error("CSRF token not found on login page")
            return {"error": "CSRF token not found"}

        if not AIRFLOW_URL or not username or not password:
            logger.error("Airflow configuration or credentials missing")
            return {"error": "Missing configuration or credentials"}

        # Step 2: Authenticate.
        login_data = urllib.parse.urlencode({
            "username": username,
            "password": password,
            "csrf_token": csrf_token,
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{AIRFLOW_URL}/login/",
            data=login_data,
            method="POST"
        )

        with opener.open(req) as resp:
            response_body = resp.read().decode("utf-8")

            # Check for common login failure responses.
            if (
                "Invalid login" in response_body
                or "incorrect" in response_body.lower()
            ):
                logger.error("Login failed")
                return {"error": "Login failed"}

            elif (
                "csrf_token" in response_body
                and "Sign In" in response_body
            ):
                logger.error("Login page returned after authentication")
                return {"error": "Authentication unsuccessful"}

        logger.info("Login request completed")

        # Step 3: Avoid duplicate DAG triggers.
        if is_dag_running(opener, dag_id):
            logger.info("DAG is already running or queued")
            return {"message": "DAG already running or queued"}

        # Step 4: Trigger the DAG.
        api_url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"

        payload = json.dumps({
            "conf": conf or {}
        }).encode("utf-8")

        headers = {
            "Content-Type": "application/json"
        }

        req = urllib.request.Request(
            api_url,
            data=payload,
            headers=headers,
            method="POST"
        )

        with opener.open(req) as dag_resp:
            response_data = json.loads(
                dag_resp.read().decode("utf-8")
            )

            logger.info("DAG triggered successfully")
            return response_data

    except urllib.error.HTTPError as exc:
        logger.error("HTTP error: %s", exc.code)
        return {"error": f"HTTP error: {exc.code}"}

    except urllib.error.URLError as exc:
        logger.error("URL connection error: %s", exc.reason)
        return {"error": "Airflow connection failed"}

    except Exception:
        logger.exception("Unexpected error while triggering DAG")
        return {"error": "Unexpected error while triggering DAG"}


def lambda_handler(event, context):
    for record in event.get("Records", []):
        object_key = record["s3"]["object"]["key"]

        conf = event.get(
            "conf",
            {"triggered_by": "demo_lambda_helper"}
        )

        for folder_prefix, dag_id in object_key_and_dag_name.items():
            if folder_prefix in object_key:
                result = login_and_trigger_dag(dag_id, conf)

                return {
                    "statusCode": 200,
                    "body": json.dumps(result)
                }

    logger.warning("No matching folder conditions found")

    return {
        "statusCode": 400,
        "body": json.dumps({
            "message": "No matching folder conditions found"
        })
    }
