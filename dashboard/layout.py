import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.graph_objs as go
from datetime import date
from data_loader import dataframe_finances

min_date = pd.to_datetime(dataframe_finances['Date']).min().date()
max_date = pd.to_datetime(dataframe_finances['Date']).max().date()
diff_date = (max_date - min_date) / 2 + min_date
def create_layout():
    return html.Div([
                dcc.Graph(
                    id='graph',
                    figure=go.Figure(data=[go.Candlestick(
    x=dataframe_finances['Date'],
    open=dataframe_finances['AAPL.Open'], high=dataframe_finances['AAPL.High'],
    low=dataframe_finances['AAPL.Low'], close=dataframe_finances['AAPL.Close'],
    increasing_line_color= 'green', decreasing_line_color= 'red'
        )])
                ),
                dcc.DatePickerRange(
        id='my-date-picker-range',
        min_date_allowed=min_date,
        max_date_allowed=max_date,
        initial_visible_month=diff_date,
        end_date=max_date,
        start_date=min_date
    ),
    html.Div(id='output-container-date-picker-range'),
    html.Button('Chandelier', id='btn-chandelier', n_clicks=0),
    html.Button('Ligne de tendance', id='btn-ligne', n_clicks=0),
    ])