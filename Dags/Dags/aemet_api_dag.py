from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from datetime import datetime, timedelta
import pandas as pd
import concurrent.futures
import time
import locale

def api_aemet_data():
    # Se define la ruta donde se guardará el archivo CSV
    path = '/home/aitor/airflow/dags/API_AEMET/'

    url = "https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones/"

    querystring = {"api_key":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJtaW5pN2ZhYmVyb0BnbWFpbC5jb20iLCJqdGkiOiI1ZmQ2Zjg1OS0xNTJhLTRiZWUtOTExZS03YjE5YWE1NTMxMjciLCJpc3MiOiJBRU1FVCIsImlhdCI6MTcxNDU4MTEzNSwidXNlcklkIjoiNWZkNmY4NTktMTUyYS00YmVlLTkxMWUtN2IxOWFhNTUzMTI3Iiwicm9sZSI6IiJ9.XVMktob6vLNYrOK2FU759KtgJo4VLJ8bQjrZNDktKP4"}

    headers = {
        'cache-control': "no-cache"
        }

    url_estaciones = requests.request("GET", url, headers=headers, params=querystring).json()['datos']
    url_estaciones_metadata = requests.request("GET", url, headers=headers, params=querystring).json()['metadatos']

    data = requests.request("GET", url_estaciones, headers=headers, params=querystring).json()
    df_estaciones = pd.DataFrame(data)
    df_estaciones.to_csv('datos.csv', index=False, header=True)

    df_estaciones.provincia.unique()

    provincia_comunidad = {

        'ARABA/ALAVA': 'País Vasco',
        'Albacete': 'Castilla-La Mancha',
        'Alicante': 'Comunidad Valenciana',
        'ALMERIA': 'Andalucía',
        'Asturias': 'Asturias',
        'AVILA': 'Castilla y León',
        'Badajoz': 'Extremadura',
        'Barcelona': 'Cataluña',
        'Burgos': 'Castilla y León',
        'CACERES': 'Extremadura',
        'CADIZ': 'Andalucía',
        'Cantabria': 'Cantabria',
        'CASTELLON': 'Comunidad Valenciana',
        'Ciudad Real': 'Castilla-La Mancha',
        'CORDOBA': 'Andalucía',
        'Cuenca': 'Castilla-La Mancha',
        'Girona': 'Cataluña',
        'Granada': 'Andalucía',
        'Guadalajara': 'Castilla-La Mancha',
        'Gipuzkoa': 'País Vasco',
        'Huelva': 'Andalucía',
        'Huesca': 'Aragón',
        'ILLES BALEARS': 'Islas Baleares',
        'JAEN': 'Andalucía',
        'A CORUÑA': 'Galicia',
        'La Rioja': 'La Rioja',
        'Las Palmas': 'Canarias',
        'LEON': 'Castilla y León',
        'LLEIDA': 'Cataluña',
        'Lugo': 'Galicia',
        'Madrid': 'Comunidad de Madrid',
        'MALAGA': 'Andalucía',
        'Murcia': 'Región de Murcia',
        'Navarra': 'Navarra',
        'OURENSE': 'Galicia',
        'Palencia': 'Castilla y León',
        'Pontevedra': 'Galicia',
        'Salamanca': 'Castilla y León',
        'SANTA CRUZ DE TENERIFE': 'Canarias',
        'Segovia': 'Castilla y León',
        'Sevilla': 'Andalucía',
        'Soria': 'Castilla y León',
        'Tarragona': 'Cataluña',
        'Teruel': 'Aragón',
        'Toledo': 'Castilla-La Mancha',
        'Valencia': 'Comunidad Valenciana',
        'Valladolid': 'Castilla y León',
        'BIZKAIA': 'País Vasco',
        'Zamora': 'Castilla y León',
        'Zaragoza': 'Aragón',
        'Ceuta': 'Ceuta',
        'Melilla': 'Melilla',
        'STA. CRUZ DE TENERIFE': 'Canarias',
        'Baleares': 'Islas Baleares',
    }
    provincia_comunidad = {k.upper(): v for k, v in provincia_comunidad.items()}
    df_estaciones['comunidad'] = df_estaciones['provincia'].map(provincia_comunidad)

    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

    api_key = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJtaW5pN2ZhYmVyb0BnbWFpbC5jb20iLCJqdGkiOiI3NzY0MzlhMi1lOTExLTQyMjEtOWNmNS1lYzk3MWNlNzFlMDQiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTcxNjQ5NzI0MywidXNlcklkIjoiNzc2NDM5YTItZTkxMS00MjIxLTljZjUtZWM5NzFjZTcxZTA0Iiwicm9sZSI6IiJ9.I2ujnha4DM1E3Ry6EUhAhzf0Tlz3hY5hnBAZmrwbyJA"
    def fetch_df_from_period(start_date, end_date, api_key):
        fechaIniStr = start_date.strftime("%Y-%m-%dT%H:%M:%SUTC")
        fechaFinStr = end_date.strftime("%Y-%m-%dT%H:%M:%SUTC")
        url = f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/{fechaFinStr}/todasestaciones"
        querystring = {"api_key": api_key}
        headers = {'cache-control': "no-cache"}

        success = False
        while not success:  # Intenta hasta que tenga éxito
            try:
                response = requests.get(url, headers=headers, params=querystring)
                response.raise_for_status()
                if response:
                    url2 = response.json()['datos']
                    data = requests.get(url2, headers=headers, params=querystring)
                    if data:
                        df = pd.DataFrame(data.json())
                        success = True
                        return df
            except requests.exceptions.ConnectionError as e:
                print("Error de conexión:", e)
                print(f'No se obtuvieron datos para el período entre {start_date.strftime("%d-%b %Y")} y {end_date.strftime("%d-%b %Y")}')
                time.sleep(5)  # Espera un poco antes de intentarlo de nuevo

        if not success:
            print(f'No se pudieron obtener datos después de múltiples intentos para el período entre {start_date.strftime("%d-%b %Y")} y {end_date.strftime("%d-%b %Y")}')
            return pd.DataFrame()  # Si no puede obtener los datos, devuelve un DataFrame vacío

    start_date = datetime(2022, 1, 1) 
    end_date = datetime(2023, 12, 31)
    # el intervalo máximo de días que deja pedir en una llamada al API de AEMET es 32 (incluyendo el primer día y el último).
    interval = 32 
    # el numero maximo de llamadas por minuto permitidas para un mismo API key de AEMET es 50.
    batch_size = int(48 / 2) # Dividimos entre 2 porque por cada intervalo de fecha son 2 requests para obtener los datos.
    timeout_between_requests = 60 # (un minuto)
    batch_start_date = start_date - timedelta(days = 1)

    all_batch_dfs = []
    while batch_start_date <= end_date:
            task_start_time = time.time()
            batch_end_date = min(batch_start_date + (batch_size) * timedelta(days=interval), end_date)
            formatted_start_date = (batch_start_date + timedelta(days = 1)).strftime("%d-%b %Y")
            formatted_end_date = batch_end_date.strftime("%d-%b %Y")
            print(f'Obteniendo datos para las fechas comprendidas entre {formatted_start_date} y {formatted_end_date}...')
            with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [
                            executor.submit(
                                    fetch_df_from_period,
                                    batch_start_date + timedelta(days=interval * (i-1)+1),
                                    min(batch_start_date + timedelta(days=interval * i), end_date),
                                    api_key
                            )
                            for i in range(1, min(batch_size, int((batch_end_date - batch_start_date).days / interval)+1)+1)
                    ]
            batch_df = pd.concat([future.result() for future in concurrent.futures.as_completed(futures)], ignore_index=True)
            all_batch_dfs.append(batch_df)
            task_end_time = time.time()
            elapsed_time = task_end_time - task_start_time
            print(f'¡Datos obtenidos!. Tiempo transcurrido: {round(elapsed_time, 3)} s.')
            batch_start_date = min(batch_end_date, end_date)
            if batch_start_date >= end_date:
                    break
            remaining_time = max(0, timeout_between_requests - (task_end_time - task_start_time))
            if remaining_time == 0:
                    # Si la llamada al API duró mas de 60 segundos, metemos un timeout de 60 segundos para el siguiente lote
                    remaining_time = 60
            while remaining_time > 0:
                    print(f'Esperando {int(remaining_time)} segundos más antes del siguiente lote...', end='\r')
                    time.sleep(1)
                    remaining_time -= 1
            print(' '*50, end='\r')
            time.sleep(5)  
    print('¡Tarea completada!')
    final_df = pd.concat(all_batch_dfs)

    df_final = final_df.merge(df_estaciones, on='indicativo', how='inner')
    df_final = df_final.sort_values('fecha')

    df_final.to_csv(path + 'dataset_1991_to_2023.csv', index=False, header=True)

with DAG(
    dag_id='aemet_api_dag',
    schedule=None,  # Manual trigger
    catchup=False,
    start_date=datetime(2023, 1, 1),  # Necesario para inicializar el DAG
) as dag:

    fetch_data = PythonOperator(
        task_id='api_aemet_data',
        python_callable=api_aemet_data,
    )