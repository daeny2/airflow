import CrawlingConfig

import psycopg2

import azure.cosmos.exceptions as exceptions
from azure.cosmos import CosmosClient

from pytz import timezone
from datetime import datetime, date, timedelta

days = ['월', '화', '수', '목', '금', '토', '일']
my_tz = timezone('Asia/Seoul')

#
# 경쟁사 정보 저장
#
def store_db_competitors(idContainer: str, pkPrefix: str, nmRentacar: str):
    try:
        # Get TODAY
        today = datetime.now(my_tz).strftime("%Y%m%d")

        # CosmosDB Information
        URL = CrawlingConfig.COSMOSDB_CONFIG['url']
        KEY = CrawlingConfig.COSMOSDB_CONFIG['key']
        DB_ID = CrawlingConfig.COSMOSDB_CONFIG['db_id']
        CT_ID = idContainer
        DB_PK_PREFIX = pkPrefix
        DB_DID_PREFIX = DB_PK_PREFIX
        RENTACAR_NAME =  nmRentacar

        # setup cosmos database
        client = CosmosClient(URL, credential=KEY)
        cosmos_db = client.get_database_client(DB_ID)

        # setup cosmos container
        container = cosmos_db.get_container_client(CT_ID)
        part_key = DB_PK_PREFIX+today[0:4]

        # Connect to PostgreSQL 
        pdb_host = CrawlingConfig.POSTGRES_CONFIG['host']
        pdb_name = CrawlingConfig.POSTGRES_CONFIG['dbname']
        pdb_user = CrawlingConfig.POSTGRES_CONFIG['user']
        pdb_pass = CrawlingConfig.POSTGRES_CONFIG['password']
        pdb_port = CrawlingConfig.POSTGRES_CONFIG['port']
        postgres_db = psycopg2.connect("host="+pdb_host+" dbname="+pdb_name+" user="+pdb_user+" password="+pdb_pass+" port="+pdb_port)

        # PostgresSQL Schema and Table Name
        pdb_scm = CrawlingConfig.COMP_INFO['pdb_schema']
        pdb_tbl = CrawlingConfig.COMP_INFO['pdb_tbl']

        # doc id 쿼리
        did_lists = list(container.query_items(query='SELECT p.id FROM Competitors p WHERE p.partitionKey = \"' + part_key + '\"', enable_cross_partition_query=True))

        for rentDays in range (1, CrawlingConfig.CONFIG['rent_days']+1):
            for n in range (0, CrawlingConfig.CONFIG['crawling_days']):
                nextday = (datetime.now(my_tz)+timedelta(n)).strftime("%Y%m%d")
                now_dt = (datetime.now(my_tz))

                doc_id = DB_DID_PREFIX+today+'_'+nextday+'_'+str(rentDays)
                print('Reading Item by Id : {0}, Next Date is {1}'.format(doc_id, nextday))

                # doc id 가 있는지 미리 점검한다
                isContinue = 0
                for m in range(0, len(did_lists)):
                    find_did = {v:k for k,v in did_lists[m].items()}
                    did = find_did.get(doc_id)
                    if did is not None:
                        isContinue = 1

                if isContinue == 0:
                    continue

                # Get data from cosmosDB
                response = container.read_item(item=doc_id, partition_key=part_key)

                print('Item read by Id {0}'.format(doc_id))
                print('Partition Key: {0}'.format(response.get('partitionKey')))
                print('Crawling Date: {0}'.format(response.get('crawling_dt')))

                wcl_dt = response.get('crawling_dt')                # 크롤링 날짜
                crawling_dt = datetime.strptime(wcl_dt, '%Y%m%d')
                rt_dt = response.get('rent_dt')
                rent_day = str(response.get('rent_day'))            # 렌트 일수(1~7)
                rent_sdt = datetime.strptime(rt_dt, '%Y%m%d')       # 렌트 시작일
                rent_edt = rent_sdt+timedelta(days=int(rent_day))   # 렌트 반납일
                rent_company = RENTACAR_NAME                        # 렌트카 회사명

                for item in response.get('items'):
                    rent_sub_company = item['rent_company']         # 렌트카 협력사명
                    car_code = item['car_code']                     # 렌트카 차량 코드
                    car_name = item['car_name']                     # 렌트카 차량 이름
                    car_kind = item['car_kind']                     # 렌트카 차종 구분
                    std_fee = str(item['car_net_fee'])              # 렌트카 표준가격
                    no_mb_fee = str(item['nm_rent_fee'])            # 렌트카 비회원 가격
                    mb_fee = str(item['mb_rent_fee'])               # 렌트카 회원가격

                    pdb_clm = 'wcl_dt, strt_prrn_dtm, arvl_prrn_dtm, rntl_dcnt, cmpt_site_nm, cmpt_entr_nm, cmpt_crty_cd,  vhfr_nm, cmpt_crty_nm, std_rntl_fee, rntl_fee, last_rntl_fee, reg_user_id, reg_pgm_id, upd_user_id, upd_pgm_id, snsh_crit_dt'
                    data = '\''+str(crawling_dt)+'\','+ '\''+str(rent_sdt)+'\',\''+str(rent_edt)+'\','+rent_day+',\''+rent_company+'\',\''+rent_sub_company+'\',\''+car_code+'\',\''+car_kind+'\',\''+car_name+'\','+std_fee+','+no_mb_fee+','+mb_fee+','+'\'SYSTEM\''+','+'\'DP_WCL\''+','+'\'SYSTEM\''+','+'\'DP_WCL\',\''+str(now_dt)+'\''
                    sql = "INSERT INTO {schema}.{table}({colum}) VALUES ({data}) ;".format(schema=pdb_scm,table=pdb_tbl,colum=pdb_clm,data=data)

                    cursor = postgres_db.cursor()
                    cursor.execute(sql)
                    postgres_db.commit()

    except exceptions.CosmosHttpResponseError as e:
        print('caught an error. {0}'.format(e.message))

    finally:
        print("done.")

#
# 제주 입도객 정보 저장
#
def store_db_visitjeju():
    try:
        # Get TODAY
        today = datetime.now(my_tz).strftime("%Y%m%d")
        yesterday = (datetime.now(my_tz)-timedelta(1)).strftime("%Y%m%d")
        now_dt = (datetime.now(my_tz))

        # CosmosDB Information
        URL = CrawlingConfig.COSMOSDB_CONFIG['url']
        KEY = CrawlingConfig.COSMOSDB_CONFIG['key']
        DB_ID = CrawlingConfig.COSMOSDB_CONFIG['db_id']
        CT_ID = CrawlingConfig.COSMOSDB_CONFIG['visitjeju_ct_id']
        DB_PK_PREFIX = CrawlingConfig.COSMOSDB_CONFIG['visitjeju_part_key_prefix']
        DB_DID_PREFIX = DB_PK_PREFIX

        # setup cosmos database
        client = CosmosClient(URL, credential=KEY)
        cosmos_db = client.get_database_client(DB_ID)

        # setup cosmos container
        container = cosmos_db.get_container_client(CT_ID)
        part_key = DB_PK_PREFIX+today[0:4]

        doc_id = DB_DID_PREFIX+yesterday
        part_key = DB_PK_PREFIX+yesterday[0:4]
        print('Reading Item by Id : {0}'.format(doc_id))

        # We can do an efficient point read lookup on partition key and id
        response = container.read_item(item=doc_id, partition_key=part_key)

        print('Item read by Id {0}'.format(doc_id))
        print('Partition Key: {0}'.format(response.get('partitionKey')))
        print('Local_day: {0}'.format(response.get('local_day')))
        print('Foreign_day: {0}'.format(response.get('foreign_day')))
        print('Local_mon: {0}'.format(response.get('local_mon')))
        print('Foreign_mon: {0}'.format(response.get('foreign_mon')))

        visit_dt = response.get('enter_dt')               # 입도일
        local_day = response.get('local_day')             # 내국인 일일 입도객 수
        foreign_day = response.get('foreign_day')         # 외국인 일일 입도객 수
        local_mon = response.get('local_mon')             # 내국인 월별 입도객 수(해당 일자까지의 합)
        foreign_mon = response.get('foreign_mon')         # 외국인 월별 입도객 수(해당 일자까지의 합)

        ipdo_dt = datetime.strptime(visit_dt, '%Y%m%d')

        # Connect to PostgreSQL 
        pdb_host = CrawlingConfig.POSTGRES_CONFIG['host']
        pdb_name = CrawlingConfig.POSTGRES_CONFIG['dbname']
        pdb_user = CrawlingConfig.POSTGRES_CONFIG['user']
        pdb_pass = CrawlingConfig.POSTGRES_CONFIG['password']
        pdb_port = CrawlingConfig.POSTGRES_CONFIG['port']
        postgres_db = psycopg2.connect("host="+pdb_host+" dbname="+pdb_name+" user="+pdb_user+" password="+pdb_pass+" port="+pdb_port)

        # PostgresSQL Schema and Table Name
        pdb_scm = CrawlingConfig.VISIT_JEJU['pdb_schema']
        pdb_tbl = CrawlingConfig.VISIT_JEJU['pdb_tbl']

        pdb_clm = 'vist_dt, lcl_dd_vist_cnt, frnr_dd_vist_cnt, lcl_mm_vist_cnt, frnr_mm_vist_cnt, reg_user_id, reg_pgm_id, upd_user_id, upd_pgm_id, snsh_crit_dt'
        data = '\''+str(ipdo_dt)+'\','+local_day+','+foreign_day+','+local_mon+','+foreign_mon+','+'\'SYSTEM\''+','+'\'DP_WCL\''+','+'\'SYSTEM\''+','+'\'DP_WCL\',\''+str(now_dt)+'\''
        sql = "INSERT INTO {schema}.{table}({colum}) VALUES ({data}) ;".format(schema=pdb_scm,table=pdb_tbl,colum=pdb_clm,data=data)
        print(sql)

        cursor = postgres_db.cursor()
        cursor.execute(sql)
        postgres_db.commit()

    except exceptions.CosmosHttpResponseError as e:
        print('caught an error. {0}'.format(e.message))

    finally:
        print("done.")

#
# 제주 입도객(항공편) 정보 저장
#
def store_db_visitjeju_air():
    try:
        # Get TODAY
        today = datetime.now(my_tz).strftime("%Y%m%d")
        yesterday = (datetime.now(my_tz)-timedelta(1)).strftime("%Y%m%d")
        now_dt = (datetime.now(my_tz))

        # CosmosDB Information
        URL = CrawlingConfig.COSMOSDB_CONFIG['url']
        KEY = CrawlingConfig.COSMOSDB_CONFIG['key']
        DB_ID = CrawlingConfig.COSMOSDB_CONFIG['db_id']
        CT_ID = CrawlingConfig.COSMOSDB_CONFIG['visitjeju_air_ct_id']
        DB_PK_PREFIX = CrawlingConfig.COSMOSDB_CONFIG['visitjeju_air_part_key_prefix']
        DB_DID_PREFIX = DB_PK_PREFIX

        # setup cosmos database
        client = CosmosClient(URL, credential=KEY)
        cosmos_db = client.get_database_client(DB_ID)

        # setup cosmos container
        container = cosmos_db.get_container_client(CT_ID)
        part_key = DB_PK_PREFIX+today[0:4]

        doc_id = DB_DID_PREFIX+yesterday
        part_key = DB_PK_PREFIX+yesterday[0:4]
        print('Reading Item by Id : {0}'.format(doc_id))

        # We can do an efficient point read lookup on partition key and id
        response = container.read_item(item=doc_id, partition_key=part_key)

        print('Item read by Id {0}'.format(doc_id))
        print('Partition Key: {0}'.format(response.get('partitionKey')))
        print('Local_day: {0}'.format(response.get('local_day')))
        print('Foreign_day: {0}'.format(response.get('foreign_day')))

        visit_dt = response.get('enter_dt')              # 입도일
        local_tot = str(response.get('local_day'))       # 내국인 일일 입도객 수
        foreign_tot = str(response.get('foreign_day'))   # 외국인 일일 입도객 수
        ipdo_dt = datetime.strptime(visit_dt, '%Y%m%d')  #

        # Connect to PostgreSQL 
        pdb_host = CrawlingConfig.POSTGRES_CONFIG['host']
        pdb_name = CrawlingConfig.POSTGRES_CONFIG['dbname']
        pdb_user = CrawlingConfig.POSTGRES_CONFIG['user']
        pdb_pass = CrawlingConfig.POSTGRES_CONFIG['password']
        pdb_port = CrawlingConfig.POSTGRES_CONFIG['port']
        postgres_db = psycopg2.connect("host="+pdb_host+" dbname="+pdb_name+" user="+pdb_user+" password="+pdb_pass+" port="+pdb_port)

        # PostgresSQL Schema and Table Name
        pdb_scm = CrawlingConfig.VISIT_JEJU_AIR['pdb_schema']
        pdb_tbl = CrawlingConfig.VISIT_JEJU_AIR['pdb_tbl']

        pdb_clm = 'vist_dt, dd_lcl_vist_cnt, dd_frnr_vist_cnt, reg_user_id, reg_pgm_id, upd_user_id, upd_pgm_id, snsh_crit_dt'
        data = '\''+str(ipdo_dt)+'\','+local_tot+','+foreign_tot+','+'\'SYSTEM\''+','+'\'DP_WCL\''+','+'\'SYSTEM\''+','+'\'DP_WCL\',\''+str(now_dt)+'\''
        sql = "INSERT INTO {schema}.{table}({colum}) VALUES ({data}) ;".format(schema=pdb_scm,table=pdb_tbl,colum=pdb_clm,data=data)
        print(sql)

        cursor = postgres_db.cursor()
        cursor.execute(sql)
        postgres_db.commit()

    except exceptions.CosmosHttpResponseError as e:
        print('caught an error. {0}'.format(e.message))

    finally:
        print("done.")

#
# 10일간 날씨 예보 정보 저장
#
def store_db_forecast_weather():
    try:
        # Get TODAY
        today = datetime.now(my_tz).strftime("%Y%m%d")
        now_dt = (datetime.now(my_tz))

        # CosmosDB Information
        URL = CrawlingConfig.COSMOSDB_CONFIG['url']
        KEY = CrawlingConfig.COSMOSDB_CONFIG['key']
        DB_ID = CrawlingConfig.COSMOSDB_CONFIG['db_id']
        CT_ID = CrawlingConfig.COSMOSDB_CONFIG['wt_forecast_ct_id']
        DB_PK_PREFIX = CrawlingConfig.COSMOSDB_CONFIG['wt_forecast_part_key_prefix']
        DB_DID_PREFIX = DB_PK_PREFIX

        # setup database
        client = CosmosClient(URL, credential=KEY)
        cosmos_db = client.get_database_client(DB_ID)

        # setup container
        container = cosmos_db.get_container_client(CT_ID)

        doc_id = DB_DID_PREFIX+today
        part_key = DB_PK_PREFIX+today[0:4]
        print('Reading Item by Id : {0}'.format(doc_id))

        # We can do an efficient point read lookup on partition key and id
        response = container.read_item(item=doc_id, partition_key=part_key)

        print('Item read by Id {0}'.format(doc_id))
        print('Partition Key: {0}'.format(response.get('partitionKey')))
        print('Crawling Date: {0}'.format(response.get('crawling_dt')))

        crawling_dt = response.get('crawling_dt')

        # Connect to PostgreSQL 
        pdb_host = CrawlingConfig.POSTGRES_CONFIG['host']
        pdb_name = CrawlingConfig.POSTGRES_CONFIG['dbname']
        pdb_user = CrawlingConfig.POSTGRES_CONFIG['user']
        pdb_pass = CrawlingConfig.POSTGRES_CONFIG['password']
        pdb_port = CrawlingConfig.POSTGRES_CONFIG['port']
        postgres_db = psycopg2.connect("host="+pdb_host+" dbname="+pdb_name+" user="+pdb_user+" password="+pdb_pass+" port="+pdb_port)

        pdb_scm = CrawlingConfig.FORECAST_WEATHER['pdb_schema']
        pdb_tbl = CrawlingConfig.FORECAST_WEATHER['pdb_tbl']

        cursor = postgres_db.cursor()

        for item in response.get('items'):
            arcu_dt = item['dt']
            wh_dt = datetime.strptime(arcu_dt, '%Y-%m-%d')  # 날짜
            wh_dt_days = days[wh_dt.weekday()]              # 요일

            wh_temp_max = str(item['temp_max'])             # 최고 온도
            wh_temp_min = str(item['temp_min'])             # 최저 온도
            wh_d_phrase = str(item['d_phrase'])             # 주간 날씨 설명
            wh_d_precip = str(item['d_precipitation'])      # 주간 강수 여부
            wh_d_rain_a = str(item['d_rain_amount'])        # 주간 강수량
            wh_d_rain_p = str(item['d_rain_prob'])          # 주간 강수 확률
            wh_d_snow_p = str(item['d_snow_prob'])          # 주간 강설 확률
            wh_d_cloudc = str(item['d_cloud_cover'])        # 주간 구름
            wh_n_phrase = str(item['n_phrase'])             # 야간 날씨 설명
            wh_n_precip = str(item['n_precipitation'])      # 야간 강수 여부
            wh_n_rain_a = str(item['n_rain_amount'])        # 야간 강수량
            wh_n_rain_p = str(item['n_rain_prob'])          # 야간 강수 확률
            wh_n_snow_p = str(item['n_snow_prob'])          # 야간 강설 확률
            wh_n_cloudc = str(item['n_cloud_cover'])        # 야간 구름

            pdb_clm = 'wthr_forc_dt, wthr_forc_wkdy_nm, max_tmpt, min_tmpt, ddt_wthr, ddt_prec_yn, ddt_prec_qty, ddt_prec_prt, ddt_snow_prt, ddt_cloud, nit_wthr, nit_prec_yn, nit_prec_qty, nit_prec_prt, nit_snow_prt, nit_cloud, reg_user_id, reg_pgm_id, upd_user_id, upd_pgm_id, snsh_crit_dt'
            data = '\''+str(wh_dt)+'\','+ '\''+wh_dt_days+'\','+wh_temp_max+','+wh_temp_min+',\''+str(wh_d_phrase)+'\','+wh_d_precip+','+wh_d_rain_a+','+wh_d_rain_p+','+wh_d_snow_p+','+wh_d_cloudc+',\''+str(wh_n_phrase)+'\','+wh_n_precip+','+wh_n_rain_a+','+wh_n_rain_p+','+wh_n_snow_p+','+wh_n_cloudc+','+'\'SYSTEM\''+','+'\'DP_WCL\''+','+'\'SYSTEM\''+','+'\'DP_WCL\',\''+str(now_dt)+'\''
            sql = "INSERT INTO {schema}.{table}({colum}) VALUES ({data}) ;".format(schema=pdb_scm,table=pdb_tbl,colum=pdb_clm,data=data)

            cursor.execute(sql)
            postgres_db.commit()

    except exceptions.CosmosHttpResponseError as e:
        print('caught an error. {0}'.format(e.message))

    finally:
        print("done.")

#
# 과거 날씨 정보 저장
#
def store_db_past_weather():
    try:
        # Get TODAY
        today = datetime.now(my_tz).strftime("%Y%m%d")
        now_dt = (datetime.now(my_tz))

        # CosmosDB Information
        URL = CrawlingConfig.COSMOSDB_CONFIG['url']
        KEY = CrawlingConfig.COSMOSDB_CONFIG['key']
        DB_ID = CrawlingConfig.COSMOSDB_CONFIG['db_id']
        CT_ID = CrawlingConfig.COSMOSDB_CONFIG['wt_past_forecast_ct_id']
        DB_PK_PREFIX = CrawlingConfig.COSMOSDB_CONFIG['wt_past_forecast_part_key_prefix']
        DB_DID_PREFIX = DB_PK_PREFIX

        # setup database
        client = CosmosClient(URL, credential=KEY)
        cosmos_db = client.get_database_client(DB_ID)

        # setup container
        container = cosmos_db.get_container_client(CT_ID)

        doc_id = DB_DID_PREFIX+today
        part_key = DB_PK_PREFIX+today[0:4]
        print('Reading Item by Id : {0}'.format(doc_id))

        # We can do an efficient point read lookup on partition key and id
        response = container.read_item(item=doc_id, partition_key=part_key)

        print('Item read by Id {0}'.format(doc_id))
        print('Partition Key: {0}'.format(response.get('partitionKey')))
        print('Crawling Date: {0}'.format(response.get('crawling_dt')))

        crawling_dt = response.get('crawling_dt')

        # Connect to PostgreSQL 
        pdb_host = CrawlingConfig.POSTGRES_CONFIG['host']
        pdb_name = CrawlingConfig.POSTGRES_CONFIG['dbname']
        pdb_user = CrawlingConfig.POSTGRES_CONFIG['user']
        pdb_pass = CrawlingConfig.POSTGRES_CONFIG['password']
        pdb_port = CrawlingConfig.POSTGRES_CONFIG['port']
        postgres_db = psycopg2.connect("host="+pdb_host+" dbname="+pdb_name+" user="+pdb_user+" password="+pdb_pass+" port="+pdb_port)

        cursor = postgres_db.cursor()

        pdb_scm = CrawlingConfig.PAST_WEATHER['pdb_schema']
        pdb_tbl = CrawlingConfig.PAST_WEATHER['pdb_tbl']

        for item in response.get('items'):
            kma_dt = item['tm']
            wh_dt = datetime.strptime(kma_dt, '%Y-%m-%d')    # 날짜
            wh_dt_days = days[wh_dt.weekday()]               # 요일

            wh_temp_max = str(item['temp_max'])              # 최고 온도
            wh_temp_min = str(item['temp_min'])              # 최저 온도
            wh_rain_amt = str(item['rain_amount'])           # 강수량
            wh_snow_amt = str(item['snow_new_amount'])       # 적설량

            pdb_clm = 'wthr_dt, wthr_wkdy_nm, max_tmpt, min_tmpt, rain_amt, snow_amt, reg_user_id, reg_pgm_id, upd_user_id, upd_pgm_id, snsh_crit_dt'
            data = '\''+str(wh_dt)+'\','+ '\''+wh_dt_days+'\','+wh_temp_max+','+wh_temp_min+','+wh_rain_amt+','+wh_snow_amt+','+'\'SYSTEM\''+','+'\'DP_WCL\''+','+'\'SYSTEM\''+','+'\'DP_WCL\',\''+str(now_dt)+'\''
            sql = "INSERT INTO {schema}.{table}({colum}) VALUES ({data}) ;".format(schema=pdb_scm,table=pdb_tbl,colum=pdb_clm,data=data)

            cursor.execute(sql)
            postgres_db.commit()

    except exceptions.CosmosHttpResponseError as e:
        print('caught an error. {0}'.format(e.message))

    finally:
        print("done.")
