import json
import logging
import sys
import boto3
import configparser
import io
import urllib.request
import urllib.parse
from html.parser import HTMLParser
logger = logging.getLogger()
logger.setLevel(logging.INFO)

object_key_and_dag_name = {"under_armour_1pd_attributes_attribute_pull/":"1pd_under_armour",
                           "under_armour_1pd_id_mapping_full/":"1pd_under_armour",
                           "under_armour_1pd_id_mapping_resolved/":"1pd_under_armour",
                           "omd_american_red_cross_1pd_attributes_attribute_pull/":"1pd_redcross",
                           "omd_american_red_cross_full/":"1pd_redcross",
                           "omd_american_red_cross_resolved/":"1pd_redcross",
                           }

                           
def get_ssm_params():
    """
    Fetch credentials and config from AWS SSM Parameter Store.
    The parameter value must be in .ini format, e.g.:

    [auds-airflow-trigger]
    username = airflow_user
    password = airflow_pass
    """
    client = boto3.client("ssm")
    response = client.get_parameter(
        Name="/auds/service",
        WithDecryption=True
    )
    param_buf = io.StringIO(response['Parameter']['Value'])
    parser = configparser.ConfigParser()
    parser.read_file(param_buf)

    config = {}
    for section in parser.sections():
        config[section] = dict(parser.items(section))
    return config

# Step 0: Get creds from SSM
ssm_config = get_ssm_params()
creds = ssm_config.get("auds-airflow-trigger", {})
AIRFLOW_URL = creds.get("airflow_url", "").replace('"', '').replace("'", '')
username = creds.get("username", "").replace('"', '').replace("'", '')
password = creds.get("password", "").replace('"', '').replace("'", '')

class CSRFParser(HTMLParser):
    """Simple parser to extract CSRF token from login page."""
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
    url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns?order_by=-execution_date&limit=5"
    with opener.open(url) as resp:
        if resp.status != 200:
            logger.error("Failed to fetch DAG runs")
            return False
        data = json.loads(resp.read().decode("utf-8"))

    dag_runs = data.get("dag_runs", [])
    for run in dag_runs:
        state = run.get("state", "").lower()
        if state in ["queued", "running"]:
            logger.warning(f"DAG is currently {state}. Not triggering.")
            return True
    return False


def login_and_trigger_dag(dag_id, conf=None):
    try:

        cookie_processor = urllib.request.HTTPCookieProcessor()
        opener = urllib.request.build_opener(cookie_processor)

        # Step 1: Get CSRF token
        csrf_token = get_csrf_token(opener)
        if not csrf_token:
            logger.error("CSRF token not found on login page")
            return {"error": "CSRF token not found"}

        if not username or not password:
            logger.error("Credentials missing in SSM parameter")
            return {"error": "Missing credentials"}

        # Step 3: Perform login
        login_data = urllib.parse.urlencode({
            "username": username,
            "password": password,
            "csrf_token": csrf_token,
        }).encode("utf-8")

        req = urllib.request.Request(f"{AIRFLOW_URL}/login/", data=login_data, method="POST")
        try:
            with opener.open(req) as resp:
                # status_code = resp.getcode()  # or resp.status
                response_body = resp.read().decode('utf-8')

                # Airflow returns HTML page for both success and failure
                if "Invalid login" in response_body or "incorrect" in response_body.lower():
                    print("Login failed: invalid credentials")
                    sys.exit(1)
                elif "csrf_token" in response_body and "Sign In" in response_body:
                    print("Login failed: still showing login page (credentials likely wrong)")
                    sys.exit(1)
                else:
                    print("Login successful or redirected to home page")
                    # Step 4: Check if DAG is already running
                    if is_dag_running(opener, dag_id):
                        print("DAG is already running or queued. Skipping trigger.")
                        return {"message": "DAG already running or queued"}

                    # Step 5: Trigger DAG
                    api_url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
                    payload = json.dumps({"conf": conf or {}}).encode("utf-8")
                    headers = {"Content-Type": "application/json"}

                    req = urllib.request.Request(api_url, data=payload, headers=headers, method="POST")
                    with opener.open(req) as dag_resp:
                        response_data = json.loads(dag_resp.read().decode("utf-8"))
                        print("DAG triggered successfully")
                        return response_data

        except urllib.error.HTTPError as e:
            print(f"HTTP error {e.code}: {e.reason}")
        except urllib.error.URLError as e:
            print(f"URL error: {e.reason}")
        except Exception as e:
            logging.error(f"{e}")
            sys.exit(1)

    except Exception as e:
        print("Error: ", str(e))
        return {"error": str(e)}

def lambda_handler(event, context):

    for record in event.get("Records", []):
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        conf = event.get("conf", {"triggered_by": "lambda_helper"})
        for key, dag_id in object_key_and_dag_name.items():
            if key in object_key:
                result = login_and_trigger_dag(dag_id, conf)
                return {
                    "statusCode": 200,
                    "body": json.dumps(result)
                }
            else:
                print(f"No matching folder condition found!!")
    # After processing all records (no matches at all)
    return {
        "statusCode": 400,
        "body": json.dumps({
            "message": "No matching folder conditions found in any records"
        })
    }