from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

download_path = "https://echo.epa.gov/files/echodownloads/"


def _epa_echo_download_links():
    download_check = requests.get(download_path)
    if download_check.status_code != 200:
        return
    download_page_html = BeautifulSoup(download_check.text, 'html.parser')
    download_links = [l["href"] for l in download_page_html.find_all("a")]
    return download_links


def _eew_manifest():
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/1Z2rBoGqb_SXW6oAu12A6TCWEJGV1pk0YxL13P_Z5Wlw/export?format=csv&gid=2049992364")
    return df.groupby("Location")["CSV FILE"].apply(list).to_dict()


def _refresh_dependencies(ti):
    manifest = ti.xcom_pull(task_ids=[
        "_eew_manifest"
    ])

    downloads = ti.xcom_pull(task_ids=[
        "_epa_echo_download_links"
    ])

    missing_data = [i for i in manifest.keys() if i not in downloads]

    if not missing_data:
        print(manifest, downloads)
#        return 'execute_download'
    print(missing_data)


with DAG("echo_data_refresh",
    start_date=datetime(2021, 10, 1), 
    schedule_interval='@daily', 
    catchup=False) as dag:

    check_echo_downloads = PythonOperator(
        task_id="epa_echo_download_links",
        python_callable=_epa_echo_download_links
    )

    get_eew_manifest = PythonOperator(
        task_id="eew_manifest",
        python_callable=_eew_manifest
    )

    refresh_dependencies = BranchPythonOperator(
        task_id="refresh_dependencies",
        python_callable=_refresh_dependencies
    )

    [check_echo_downloads, get_eew_manifest] >> refresh_dependencies