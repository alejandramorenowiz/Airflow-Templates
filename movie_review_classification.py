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
#VIRTUAL_CLUSTER_ID = os.getenv("VIRTUAL_CLUSTER_ID", "virtual_cluster_emroneks_am")
#JOB_ROLE_ARN = os.getenv("JOB_ROLE_ARN", "arn:aws:iam::306718468668:user/aws_EMROnEKS_user")
# [END emr_eks_env_variables]


# [START emr_eks_config]
JOB_DRIVER_ARG = {
    "sparkSubmitJobDriver": {
        "entryPoint": "s3://spark-jobscripts/movie_review_logic.py",
        "sparkSubmitParameters": "--conf spark.executors.instances=1"
                                 " --conf spark.executors.memory=2G"
                                 " --conf spark.executor.cores=1"
                                 " --conf spark.driver.cores=1"
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
                "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",  # noqa: E501
                #OR
                #"spark.dynamicAllocation.enabled": "false", "spark.kubernetes.executor.deleteOnTermination": "true", "spark.kubernetes.container.image": #spark_image, "spark.hadoop.fs.s3a.multiobjectdelete.enable": "false"
            },
        }
    ],
    "monitoringConfiguration": {
        "cloudWatchMonitoringConfiguration": {
            "logGroupName": "/aws/emr-eks-spark",
            "logStreamNamePrefix": "airflow",
        }
        #OR
        # "s3MonitoringConfiguration": {
        #    "logUri": "s3://spark-test"
        #}
    },
}
# [END emr_eks_config]

default_args = {
    'owner': 'alejandra.moreno',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('dag__moviereview_job', 
        default_args = default_args,
        description='Executes movie review logic',
        schedule_interval='@once',        
        catchup=False)

movie_review_job = EMRContainerOperator(
                            task_id = 'dag_movie_review_job',
                            virtual_cluster_id="virtual_cluster_emroneks_am",
                            execution_role_arn="arn:aws:iam::306718468668:user/aws_EMROnEKS_user",                   
                            release_label="emr-6.3.0-latest",
                            job_driver=JOB_DRIVER_ARG,
                            configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
                            name="movie_reviews.py",
                            dag = dag
)

movie_review_job
# https://airflow.apache.org/docs/apache-airflow-providers-amazon/2.2.0/operators/emr_eks.html#prerequisite-tasks
# https://airflow.apache.org/docs/apache-airflow-providers-amazon/2.2.0/_modules/airflow/providers/amazon/aws/example_dags/example_emr_eks_job.html
