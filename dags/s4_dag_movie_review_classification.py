import sys
import os.path
import io
from datetime import timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
import airflow.utils.dates

from airflow.providers.amazon.aws.operators.emr_containers import EMRContainerOperator


# Stopper  - How to save environment variables
# [START emr_eks_env_variables]
VIRTUAL_CLUSTER_ID = '{{conn.aws_default.extra_dejson["virtual_cluster_id"]}}'
JOB_ROLE_ARN = '{{conn.aws_default.extra_dejson["job_execution_role"]}}'
# [END emr_eks_env_variables]


# [START emr_eks_config]
JOB_DRIVER_ARG = {
    "sparkSubmitJobDriver": {
        "entryPoint": "s3://spark-jobscripts/test.py",
        "sparkSubmitParameters": "--conf spark.executors.instances=2"
                                 " --conf spark.executors.memory=2G"
                                 " --conf spark.executor.cores=2"
                                 " --conf spark.driver.cores=1"
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
            #   "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",  # noqa: E50
                "spark.dynamicAllocation.enabled": "false",
                "spark.kubernetes.executor.deleteOnTermination": "true"
            },
        }
        #{
        #    "classification": "spark-defaults",
        #    "properties": {
        #        "spark.dynamicAllocation.enabled": "false",
        #        "spark.kubernetes.executor.deleteOnTermination": "true",
        #        "spark.kubernetes.container.image": spark_image,
        #        "spark.hadoop.fs.s3a.multiobjectdelete.enable": "false"
        #    }
        #}
    ],
    "monitoringConfiguration": {
        #"cloudWatchMonitoringConfiguration": {
        #    "logGroupName": "/aws/emr-eks-spark",
        #    "logStreamNamePrefix": "airflow",
        #}
        #OR
         "s3MonitoringConfiguration": {
            "logUri": "s3://customerbucketam"
        }
    },
}
# [END emr_eks_config]

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('s4_dag_movie_review_classification', 
        default_args = default_args,
        description='Executes movie review logic',
        schedule_interval='@once',        
        catchup=False)

movie_review_job = EMRContainerOperator(
                            task_id = 'dag_movie_review_job',
                            virtual_cluster_id=VIRTUAL_CLUSTER_ID,
                            execution_role_arn=JOB_ROLE_ARN,  
                            max_tries=None,
                            release_label="emr-6.3.0-latest",
                            job_driver=JOB_DRIVER_ARG,
                            configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
                            name="movie_reviews.py",
                            dag = dag
)

movie_review_job
# https://airflow.apache.org/docs/apache-airflow-providers-amazon/2.2.0/operators/emr_eks.html#prerequisite-tasks
# https://airflow.apache.org/docs/apache-airflow-providers-amazon/2.2.0/_modules/airflow/providers/amazon/aws/example_dags/example_emr_eks_job.html
