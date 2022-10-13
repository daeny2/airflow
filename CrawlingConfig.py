# config.py
COSMOSDB_CONFIG = {
    'url': 'https://clouflake.documents.azure.com:443/',
    'key': 'Mifz2dwGXV36QI0XDsWwi4jAmkAZLJyDj8772XKJed33CVbbbOHHPwrVqHftUtjRtfdgHweaMF7aAJ9PXiiYsg==',
    'db_id': 'SKR',
    'crawling_ct_id': 'Competitors',
    'wt_forecast_ct_id': 'ForecastWeather',
    'wt_past_forecast_ct_id': 'PastWeather',
    'visitjeju_ct_id': 'VisitJeju',
    'visitjeju_air_ct_id': 'VisitAirJeju',
    'visitjeju_part_key_prefix': 'VISITJEJU_',
    'visitjeju_air_part_key_prefix': 'VISITJEJUAIR_',
    'wt_forecast_part_key_prefix': 'FORECAST_',
    'wt_past_forecast_part_key_prefix': 'WEATHER_',
    'jarrent_part_key_prefix': 'JAR_',
    'jeju_part_key_prefix': 'JEJU_',
    'jejupass_part_key_prefix': 'JEJUPASS_',
    'jejussok_part_key_prefix': 'JEJUSSOK_',
    'login_part_key_prefix': 'LOGIN_',
    'lotte_part_key_prefix': 'LOTTE_',
    'rainbow_part_key_prefix': 'RAINBOW_'
}

POSTGRES_CONFIG = {
    'host': 'skr-wave.postgres.database.azure.com',
    'dbname': 'postgres',
    'user': 'clouflake@skr-wave',
    'password': 'cf@2022#1234',
    'port': '5432'
}

VISIT_JEJU = {
    'url': 'http://www.visitjeju.or.kr',
    'pdb_schema': 'skrwave',
    'pdb_tbl': 'dp_wcl_jeju_vist_h'
}

VISIT_JEJU_AIR = {
    'url': 'https://www.airport.co.kr/www/ajaxf/frFlightStatsSvc/dailyExpectList.do',
    'pdb_schema': 'skrwave',
    'pdb_tbl': 'dp_wcl_jeju_air_vist_h'
}

FORECAST_WEATHER = {
    'arcu_url': 'http://dataservice.accuweather.com/forecasts/v1/daily/5day/3430597',
    'arcu_api_key': 'VAojweG9vQukqJyyFqNGaD8XeOwGQGcY',
    'kma1_url': 'http://apis.data.go.kr/1360000/MidFcstInfoService/getMidTa',
    'kma2_url': 'http://apis.data.go.kr/1360000/MidFcstInfoService/getMidLandFcst',
    'kma_api_key': 'h1iK4Kpj5a32BmqHOMY46bvZ92pZQEyqPOdtBoX5YU8PtOrC%2FteleouCyebB3T08%2Fq%2FtEvdHvSSKcH3ei7LJiw%3D%3D',
    'pdb_schema': 'skrwave',
    'pdb_tbl': 'dp_wcl_wthr_forc_h'
}

PAST_WEATHER = {
    'url': 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList',
    'api_key': 'h1iK4Kpj5a32BmqHOMY46bvZ92pZQEyqPOdtBoX5YU8PtOrC%2FteleouCyebB3T08%2Fq%2FtEvdHvSSKcH3ei7LJiw%3D%3D',
    'pdb_schema': 'skrwave',
    'pdb_tbl': 'dp_wcl_wthr_h'
}

COMP_INFO = {
    'pdb_schema': 'skrwave',
    'pdb_tbl': 'dp_wcl_cmpt_prc_h',
    'lotte_name': '롯데렌트카',
    'jarrent_name': '제주공항렌트카',
    'jeju_name': '제주렌트카',
    'jejupass_name': '제주패스',
    'jejussok_name': '제주항공렌트카',
    'rainbow_name': '무지개렌트카',
    'login_name': '로그인렌트카'
}

CONFIG = {
    'rent_days': 1,      # 최대 렌트 일 수 : 7
    'crawling_days': 90  # 최대 크롤링 일 수 : 90
}
