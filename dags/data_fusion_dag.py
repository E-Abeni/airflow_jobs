from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "Data_Fusion_Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
) as dag:
    
    data_cleaning = BashOperator(
        task_id="data_cleaning",
        bash_command="'/mnt/c/Users/Guest User/Desktop/airflow_jobs/.venv/bin/python3' '/mnt/c/Users/Guest User/Desktop/airflow_jobs/spark_job_data_cleaning.py'",
        dag = dag
    )


    identity_resolution = BashOperator(
        task_id="identity_resolution",
        bash_command="'/mnt/c/Users/Guest User/Desktop/airflow_jobs/.venv/bin/python3' '/mnt/c/Users/Guest User/Desktop/airflow_jobs/spark_job_identity_resolution.py'",
        dag = dag
    )



    user_profile = BashOperator(
        task_id="user_profile",
        bash_command="'/mnt/c/Users/Guest User/Desktop/airflow_jobs/.venv/bin/python3' '/mnt/c/Users/Guest User/Desktop/airflow_jobs/spark_job_user_profiles.py'",
        dag = dag
    )



    temporal_analysis = BashOperator(
        task_id="temporal_analysis",
        bash_command="'/mnt/c/Users/Guest User/Desktop/airflow_jobs/.venv/bin/python3' '/mnt/c/Users/Guest User/Desktop/airflow_jobs/spark_job_temporal_analysis.py'",
        dag = dag
    )


    network_analysis = BashOperator(
        task_id="network_analysis",
        bash_command="'/mnt/c/Users/Guest User/Desktop/airflow_jobs/.venv/bin/python3' '/mnt/c/Users/Guest User/Desktop/airflow_jobs/spark_job_network_analysis.py'",
        dag = dag
    )


    risk_scoring = BashOperator(
        task_id="risk_scoring",
        bash_command="'/mnt/c/Users/Guest User/Desktop/airflow_jobs/.venv/bin/python3' '/mnt/c/Users/Guest User/Desktop/airflow_jobs/spark_job_risk_scoring.py'",
        dag = dag
    )


    transaction_risk_analysis = BashOperator(
        task_id="transaction_risk_analysis",
        bash_command="'/mnt/c/Users/Guest User/Desktop/airflow_jobs/.venv/bin/python3' '/mnt/c/Users/Guest User/Desktop/airflow_jobs/spark_job_transaction_risk_analysis.py'",
        dag = dag
    )


    # Define task dependencies
    data_cleaning >> identity_resolution >> [user_profile , temporal_analysis, network_analysis] >> risk_scoring >> transaction_risk_analysis