import requests
import pandas as pd
from airflow import DAG
from sqlalchemy.engine import URL
from sqlalchemy import text
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
import datetime
import logging


default_args={
    'owner':'Obarskaya Tatiana',
    'email':'obarskaia.tatiana@gmail.com',
    'start_date': datetime.datetime(2023, 11, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries':1,
    #'retries_delay':datetime.timedelta(minutes=5),
}


DOMEN = 'www.wildberries.ru'
POSTGRES_DB = URL.create(drivername='postgresql', port='5432',
                         username="postgres", password="postgres",
                         host="localhost", database="postgres")
def get_pvz_info(domen):
    url = f'https://{domen}/webapi/spa/modules/pickups'
    headers = {'User-Agent': "Mozilla/5.0", 'content-type': "application/json", 'x-requested-with': 'XMLHttpRequest'}
    r = requests.get(url, headers=headers)
    data = r.json()
    interest_keys = ['id', 'address', 'coordinates', 'fittingRooms', 'workTime', 'isWb',
                    'pickupType', 'dtype', 'isExternalPostamat', 'status', 'deliveryPrice']
    all_pvz_data = []
    for d in data['value']['pickups']:
        pvz = {}
        for key in interest_keys:
            try:
                pvz[key] = d[key]
            except KeyError:
                pvz[key] = None
        all_pvz_data.append(pvz)
    all_pvz_data = pd.DataFrame(all_pvz_data)
    print("[INFO] координаты точек выдачи получены")
    return all_pvz_data

def write_pvz_info(all_pvz_data, engine):
    logging.info('Reading previous wb_points')
    sql_all_pvz_data = """select * from public.wb_points"""
    already_written = pd.DataFrame(engine.execute(text(sql_all_pvz_data)))
    if already_written.shape[0] > 0:
        logging.info('Previous wb points are found in DB')
        db_wb_points = set(list(already_written.office_id.values.astype(int)))
    else:
        db_wb_points = set()
    new_wb_points = set(list(all_pvz_data.office_id.values))
    new_points_to_write = new_wb_points.difference(db_wb_points)
    new_pvz_data = all_pvz_data[all_pvz_data.office_id.isin(list(new_points_to_write))]
    new_pvz_data = new_pvz_data.drop(['status', 'deliveryPrice'], axis=1)
    new_pvz_data.to_sql('wb_points', if_exists='append', index=False,
                        con=engine, schema='public')
    logging.info('New wb points are written to DB')
    print("[INFO] координаты точек записаны в БД")

def write_pvz_status(all_pvz_data, engine):
    logging.info('Starting of status function')
    pvz_status = all_pvz_data[['office_id', 'status', 'deliveryPrice']].copy()
    time_now = str(pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    pvz_status.to_sql('pvz_status', if_exists='append', index=False,
                      con=engine, schema='public')
    logging.info('PVZ Status Info is written to DB')
    sql_datetime = f"""update public.pvz_status set dt = TIMESTAMP '{time_now}'
                        where dt is null """
    engine.execute(text(sql_datetime))

def _pipeline():
    logging.info('Program has started. We will request wb points')
    all_pvz_data = get_pvz_info(domen=DOMEN)
    if all_pvz_data.shape[0] > 0:
        logging.info('We got all wb pvz coordinates')
    payload_ids = all_pvz_data.id.to_list()
    all_pvz_data = all_pvz_data.rename(columns={'id': 'office_id'})
    all_pvz_data['lat'] = all_pvz_data['coordinates'].apply(lambda x: x[0])
    all_pvz_data['lon'] = all_pvz_data['coordinates'].apply(lambda x: x[1])
    all_pvz_data = all_pvz_data.drop('coordinates', axis=1)
    logging.info('We are about to create postgres engine')
    engine = create_engine(POSTGRES_DB)
    if engine:
        logging.info('Engine was succesfully created')
    write_pvz_info(all_pvz_data, engine)
    write_pvz_status(all_pvz_data, engine)

with DAG(
    dag_id='wb_parse_status',
    default_args=default_args,
    description='Parsing wb pvz statuses and coordinates',
    schedule=None, #"0 0-23/4 * * *",
    catchup=False,
) as dag:

    wb_parsing=PythonOperator(
        task_id = 'pipeline',
        python_callable=_pipeline,
    )
    wb_parsing
