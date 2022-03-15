import airflow.utils.dates
from airflow import DAG

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.sensors.emr_step_sensor import  EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

BUCKET_NAME = "customerbucketam"
s3_data = "movie_review.csv"
s3_script = "s3://spark-jobscripts/user_purchase_parquet.py"
s3_clean = "processed/reviews/"
logs_location = "logs"


SPARK_STEPS = [ # Note the params values are supplied to the operator
   
    {
        "Name": "Classify log reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--packages",
                "com.databricks:spark-xml_2.12:0.14.0",
                "s3://spark-jobscripts/user_purchase_parquet.py",
            ],
        },
    }  
]


JOB_FLOW_OVERRIDES = {
    "Name": "Logs review classifier",
    "ReleaseLabel": "emr-6.5.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "LogUri" : f"s3://{BUCKET_NAME}/{logs_location}",
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    'Steps': SPARK_STEPS,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('s8_dag_user_purchase_parquet', 
        default_args = default_args,
        description='Executes logs review logic',
        schedule_interval='@once')

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Check job flow
job_sensor = EmrJobFlowSensor(task_id='check_job_flow',
 job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
 dag = dag)

create_emr_cluster >> job_sensor