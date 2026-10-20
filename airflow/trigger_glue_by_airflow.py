from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.task_group import TaskGroup
import boto3
from datetime import timedelta
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

AWS_CONN_ID = 'aws_default'
GLUE_ROLE = "test"
dag_name = "dag_name"

dag_vars = Variable.get(dag_name, deserialize_json=True)
script_location=dag_vars['script_location']
config_path = dag_vars['config_path']
external_crossaccount_catalog_id=dag_vars['external_crossaccount_catalog_id']
internal_catalog_id=dag_vars['internal_catalog_id']
table_names=dag_vars['table_names']
schema=dag_vars['schema']
input_file_path=dag_vars['input_file_path']
ice_dest_folder=dag_vars['ice_dest_folder']
catalog=dag_vars['catalog']
dataset=dag_vars['dataset']
access_emails=dag_vars['access_emails']
to_emails=dag_vars['to_emails']
cc_emails=dag_vars['cc_emails']

exec_date = '{{ (next_execution_date.replace(day=20)).strftime("%Y-%m-%d") }}'


GLUE_JOBS_CONFIG = [
    {
        "job_name": "<client>_job",
        "script_location": script_location,
        "default_args": {
            "--TempDir": "s3://<bucket>/tmp/",
            "--enable-metrics": "",
            "--job-bookmark-option": "job-bookmark-enable",
            "--external_crossaccount_catalog_id": external_crossaccount_catalog_id,
            "--internal_catalog_id": internal_catalog_id,
            "--table_names": table_names,
            "--schema": schema,
            "--input_file_path": input_file_path,
            "--ice_dest_folder": ice_dest_folder,
            "--catalog": catalog,
            "--dataset": dataset,
            "--access_emails": access_emails,
            "--to_emails": to_emails,
            "--cc_emails": cc_emails,
            "--exec_date": exec_date,
            "--config_path": config_path,
        }
    }
]

def create_or_update_glue_job(job_name, script_location, default_args):
    client = boto3.client("glue")
    job_args = {
        "Name": job_name,
        "Role": GLUE_ROLE,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3"
        },
        "DefaultArguments": default_args,
        "WorkerType": "G.2X",
        "NumberOfWorkers": 2,
        "GlueVersion": "5.0",
    }
    try:
        client.create_job(**job_args)
    except client.exceptions.IdempotentParameterMismatchException:
        update_args = job_args.copy()
        update_args.pop("Name")
        client.update_job(JobName=job_name, JobUpdate=update_args)
    except client.exceptions.AlreadyExistsException:
        pass

with DAG(
    dag_id=dag_name,
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["glue", "aws"],
    max_active_runs=1,
) as dag:

    sleep = TimeDeltaSensor(
        task_id="sleep",
        delta=timedelta(minutes=30),
        poke_interval=300,
        mode="reschedule",
    )

    end = DummyOperator(task_id="end")

    # S3 Sensor TaskGroup (parallel sensors)
    '''with TaskGroup(group_id=f's3_sensor_group_{dag_name}') as s3_sensor_tgroup:
        s3_sensor_tasks = []
        for job_cfg in GLUE_JOBS_CONFIG:
            input_url = job_cfg["default_args"]["--input_url"].rstrip("/")
            _, bucket, *prefix_parts = input_url.split("/", 3)
            prefix = prefix_parts[0] if prefix_parts else ""
            sensor = S3KeySensor(
                dag=dag,
                task_id=f"{job_cfg['job_name']}_src_sensor",
                poke_interval=60 * 60,
                timeout=60 * 60 * 24,
                bucket_name=bucket,
                bucket_key=f"{prefix}*",
                wildcard_match=True,
            )
            s3_sensor_tasks.append(sensor)'''

    # Glue jobs TaskGroup (sequential jobs)
    with TaskGroup(group_id=f'step_adder_group_{dag_name}') as step_adder_tgroup:
        previous_task = None
        for job_cfg in GLUE_JOBS_CONFIG:
            
            run_task_id = f"run_glue_{job_cfg['job_name']}"  
            run_task_id_with_grp = f"{step_adder_tgroup.group_id}.{run_task_id}" 
            
            print("run_task_id:", run_task_id)  #added new line
            create_job = PythonOperator(
                task_id=f"create_glue_job_{job_cfg['job_name']}",
                python_callable=create_or_update_glue_job,
                op_kwargs={
                    "job_name": job_cfg["job_name"],
                    "script_location": job_cfg["script_location"],
                    "default_args": job_cfg["default_args"],
                },
            )
            run_glue = GlueJobOperator(
                task_id=run_task_id,
                job_name=job_cfg["job_name"],
                wait_for_completion=False,
                verbose=True,
                num_of_dpus=2,
            )
            wait_for_glue = GlueJobSensor(
                task_id=f"wait_for_glue_{job_cfg['job_name']}",
                job_name=job_cfg["job_name"],
                run_id=f"{{{{ task_instance.xcom_pull(task_ids='{run_task_id_with_grp}') }}}}",
                mode="reschedule",
                poke_interval=60,
                timeout=3600,
            )
            if previous_task is None:
                # First job in sequence
                create_job >> run_glue >> wait_for_glue
                first_step = create_job
                last_step = wait_for_glue
            else:
                previous_task >> create_job
                create_job >> run_glue >> wait_for_glue
                last_step = wait_for_glue
            previous_task = wait_for_glue

    # Set dependencies between groups and end
    #sleep >> s3_sensor_tgroup >> step_adder_tgroup >> end
    sleep >> step_adder_tgroup >> end
