from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from sklearn.linear_model import LinearRegression
import multiprocessing
import time

def run_dash_server():
    # Se define la ruta donde se guardará el archivo CSV
    path = '/home/aitor/airflow/dags/API_AEMET/'

    final_df = pd.read_csv(path + 'historico.csv')
    final_df['fecha'] = pd.to_datetime(final_df['fecha'])

    # Dashboard
    unique_years = pd.to_datetime(final_df['fecha']).dt.year.unique()
    comunidades = final_df['comunidad'].unique()
    provincias = final_df['provincia'].unique()

    app = dash.Dash(__name__)
    app.layout = html.Div([

        html.Div([
            dcc.RadioItems(
                id='filter-type',
                options=[{'label': 'Comunidad', 'value': 'comunidad'}, {'label': 'Provincia', 'value': 'provincia'}],
                value='provincia'
            ),
            dcc.RadioItems(
                id='temp-or-prec',
                options=[{'label': 'Temperatura', 'value': 'temperatura'}, {'label': 'Precipitación', 'value': 'precipitacion'}],
                value='temperatura'
            ),
        ], style={'display': 'flex', 'flex-direction': 'row'}),

        html.Div([
            dcc.Dropdown(
                id='comunidad-dropdown',
                options=[{'label': com, 'value': com} for com in final_df['comunidad'].unique()],
                value='Castilla y León',
                clearable=False
            ),
        ], style={'width': '50%', 'display': 'inline-block'}),

        html.Div([
            dcc.Dropdown(
                id='provincia-dropdown',
                options=[{'label': prov, 'value': prov} for prov in final_df['provincia'].unique()],
                value='LEON',
                clearable=False
            ),
        ], style={'width': '50%', 'display': 'inline-block'}),

        html.Div([
            dcc.Dropdown(
                id='temp-dropdown',
                options=[{'label': 'Minimas', 'value': 'Minimas'}, {'label': 'Maximas', 'value': 'Maximas'}, {'label': 'Medias', 'value': 'Medias'}],
                value='Medias',
                clearable=False
            ),
        ], style={'width': '50%', 'display': 'inline-block'}),

        html.Div([
            dcc.Dropdown(
                id='data-frequency-dropdown',
                options=[{'label': 'Diario', 'value': 'diarias'}, {'label': 'Mensual', 'value': 'mensuales'}, {'label': 'Anual', 'value': 'anuales'}],
                value='diarias',
                clearable=False
            ),
        ], style={'width': '50%', 'display': 'inline-block'}),

        html.Div([
            dcc.RangeSlider(
                id='year-slider',
                min=unique_years.min(),
                max=unique_years.max(),
                value=[unique_years.min(), unique_years.max()],
                marks={str(year): str(year) for year in unique_years}
            )
        ], style={'width': '100%', 'margin': 'auto'}),

        dcc.Graph(id='temperature-graph'),
    ])

    @app.callback(
        Output('temperature-graph', 'figure'),
        [Input('temp-dropdown', 'value'),
         Input('provincia-dropdown', 'value'),
         Input('comunidad-dropdown', 'value'),
         Input('year-slider', 'value'),
         Input('filter-type', 'value'),
         Input('temp-or-prec', 'value'),
         Input('data-frequency-dropdown', 'value')]
    )
    def update_graph(selected_temp, selected_provincia, selected_comunidad, selected_years, filter_type, selected_temp_or_prec, data_frequency):
        if filter_type == 'comunidad':
            filtered_df = final_df[(final_df['comunidad'] == selected_comunidad) & (final_df['fecha'].dt.year.between(selected_years[0], selected_years[1]))].copy()
        else:
            filtered_df = final_df[(final_df['provincia'] == selected_provincia) & (final_df['fecha'].dt.year.between(selected_years[0], selected_years[1]))].copy()

        if selected_temp_or_prec == 'temperatura':
            if selected_temp == 'Minimas':
                column = 'tmin'
            elif selected_temp == 'Maximas':
                column = 'tmax'
            elif selected_temp == 'Medias':
                column = 'tmed'
            y_title = 'Temperatura (ºC)'
        elif selected_temp_or_prec == 'precipitacion':
            column = 'prec'
            y_title = 'Precipitación (mm)'
            filtered_df = filtered_df.dropna(subset=['prec'])
        else:
            raise ValueError('selected_temp_or_prec debe ser "temperatura" o "precipitacion"')

        filtered_df['fecha'] = pd.to_datetime(filtered_df['fecha'])
        filtered_df['year'] = filtered_df['fecha'].dt.year
        filtered_df['month'] = filtered_df['fecha'].dt.month

        if data_frequency == 'diarias':
            data = filtered_df.groupby('fecha')[column].mean().reset_index()
            data['year_month'] = data['fecha'].dt.strftime('%Y-%m')
            x_valor = 'fecha'
        elif data_frequency == 'mensuales':
            data = filtered_df.groupby(['year', 'month'])[column].mean().reset_index()
            data['year_month'] = data['year'].astype(str) + '-' + data['month'].astype(str).str.zfill(2)
            x_valor = 'year_month'
        elif data_frequency == 'anuales':
            data = filtered_df.groupby(['year'])[column].mean().reset_index()
            data['year_month'] = data['year'].astype(str)
            x_valor = 'year_month'
        else:
            raise ValueError('data_frequency debe ser "diarias", "mensuales" o "anuales"')

        fig = px.line(data, x=x_valor, y=column, title=f'Promedio {selected_temp_or_prec} {selected_temp.lower()} {data_frequency.lower()} de {selected_provincia if filter_type == "provincia" else selected_comunidad}', color_discrete_sequence=['blue'])

        X = data.index.values.reshape(-1, 1)
        y = data[column]
        model = LinearRegression()
        model.fit(X, y)
        y_pred = model.predict(X)
        data['y_pred'] = y_pred
        diff = data.y_pred.iloc[-1] - data.y_pred.iloc[0]
        if selected_temp_or_prec == 'temperatura':
            fig.add_trace(go.Scatter(x=data['year_month'], y=data['y_pred'], mode='lines', name=f'Trendline ({diff:+.2f}ºC)', line=dict(color='red', width=2)))
        else:
            fig.add_trace(go.Scatter(x=data['year_month'], y=data['y_pred'], mode='lines', name=f'Trendline ({diff:+.2f}mm)', line=dict(color='red', width=2)))

        fig.update_layout(
            plot_bgcolor='rgba(1, 1, 1, 0.05)',
            xaxis_title='Año',
            yaxis_title=y_title,
        )

        return fig

    @app.callback(
        Output('temp-dropdown', 'options'),
        [Input('temp-or-prec', 'value')]
    )
    def update_temp_dropdown(selected_temp_or_prec):
        if selected_temp_or_prec == 'temperatura':
            return [{'label': 'Minimas', 'value': 'Minimas'}, {'label': 'Maximas', 'value': 'Maximas'}, {'label': 'Medias', 'value': 'Medias'}]
        elif selected_temp_or_prec == 'precipitacion':
            return [{'label': 'Precipitación', 'value': 'precipitacion'}]

    @app.callback(
        Output('provincia-dropdown', 'options'),
        [Input('filter-type', 'value')]
    )
    def update_provincia_dropdown(filter_type):
        if filter_type == 'provincia':
            return [{'label': provincia, 'value': provincia} for provincia in provincias]
        elif filter_type == 'comunidad':
            return []

    @app.callback(
        Output('comunidad-dropdown', 'options'),
        [Input('filter-type', 'value')]
    )
    def update_comunidad_dropdown(filter_type):
        if filter_type == 'comunidad':
            return [{'label': comunidad, 'value': comunidad} for comunidad in comunidades]
        elif filter_type == 'provincia':
            return []

    app.run_server(debug=True, port=8050, host='0.0.0.0')

def start_dash_app():
    p = multiprocessing.Process(target=run_dash_server)
    p.start()
    time.sleep(300) #5 minutos
    p.terminate()
    p.join()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='visualization_dag',
    default_args=default_args,
    description='A simple visualization DAG',
    schedule='@daily', 
    catchup=False,
) as dag:

    run_dash = PythonOperator(
        task_id='run_dash',
        python_callable=start_dash_app,
    )

    run_dash
