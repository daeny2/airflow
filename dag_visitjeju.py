import CrawlingConfig
import InterfacePostgres

import json
import requests
import pendulum

from bs4 import BeautifulSoup
from pytz import timezone
from datetime import datetime, date, timedelta

import psycopg2

import azure.cosmos.exceptions as exceptions
from azure.cosmos import CosmosClient
from azure.cosmos.partition_key import PartitionKey

from airflow.decorators import dag, task


KST = pendulum.timezone("Asia/Seoul")

tags = ['local_day', 'foreign_day', 'local_mon', 'foreign_mon', 'local_tot', 'foreign_tot']

default_args = {
    'owner': 'airflow',
    'description': 'Visit Jeju airflow DAG',
    'start_date': datetime(2022, 10, 11, tzinfo=KST),
}

@dag('skr_visitjeju', default_args=default_args, schedule_interval='55 8 * * *', tags=['skr_visitjeju'])
def skr_visitjeju():
    @task()
    def crawling_site() -> list:
        try:
            my_tz = timezone('Asia/Seoul')
            today = datetime.now(my_tz).strftime("%Y%m%d")
            yesterday = (datetime.now(my_tz)-timedelta(1)).strftime("%Y%m%d")

            webpage = requests.get(CrawlingConfig.VISIT_JEJU['url'])
            soup = BeautifulSoup(webpage.content , 'html.parser')

            prd_Name = soup.findAll("span", {"class": "number"})
            n = 0

            DB_PK_PREFIX = CrawlingConfig.COSMOSDB_CONFIG['visitjeju_part_key_prefix']
            DB_DID_PREFIX = DB_PK_PREFIX

            enter_cnt_list = {}

            enter_cnt_list['id'] = DB_DID_PREFIX+yesterday
            enter_cnt_list['partitionKey'] = DB_PK_PREFIX+yesterday[0:4]
            enter_cnt_list['crawling_dt'] = today
            enter_cnt_list['enter_dt'] = yesterday

            for i in prd_Name:
                prd_name = []
                s = str(i).split(sep=' ')
                for j in range (2, len(s)-1, 2):
                  prd_name.append(s[j][5])

                cnt = ''.join(prd_name)
                enter_cnt_list[tags[n]] = cnt
                n = n + 1

        finally:
            print("crawling done.")
            return enter_cnt_list

    @task()
    def store_cosmosdb(cnt_list: list) -> int:
        try:
            # CosmosDB Information
            URL = CrawlingConfig.COSMOSDB_CONFIG['url']
            KEY = CrawlingConfig.COSMOSDB_CONFIG['key']
            DB_ID = CrawlingConfig.COSMOSDB_CONFIG['db_id']
            CT_ID = CrawlingConfig.COSMOSDB_CONFIG['visitjeju_ct_id']

            # setup database
            client = CosmosClient(URL, credential=KEY)
            db = client.get_database_client(DB_ID)

            # setup container
            try:
                container = db.create_container(id=CT_ID, partition_key=PartitionKey(path='/partitionKey'))
                print('Container with id \'{0}\' created.'.format(CT_ID))

            except exceptions.CosmosResourceExistsError:
                container = db.get_container_client(CT_ID)
                print('Container with id \'{0}\' was found.'.format(CT_ID))

            print('Creating Items.')
            container.create_item(body=cnt_list)

        except exceptions.CosmosHttpResponseError as e:
            print('caught an error. {0}.'.format(e.message))

        finally:
            print("store cosmosdb done.")
            return 1

    @task()
    def store_postgres(n: int):
        try:
            InterfacePostgres.store_db_visitjeju()

        except Exception as e:
            print("Got unhandled exception %s" % str(e))

        finally:
            print("done.")


    cnt_list = crawling_site()
    n = store_cosmosdb(cnt_list)
    store_postgres(n)

af_visitjeju_dag = skr_visitjeju()
