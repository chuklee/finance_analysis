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
    increasing_line_color= 'green', decreasing_line_color= 'red',
    

        )],
            layout= create_background()
            )
        ),
        dcc.DatePickerRange(
        id='my-date-picker-range',
        min_date_allowed=min_date,
        max_date_allowed=max_date,
        initial_visible_month=diff_date,
        end_date=max_date,
        start_date=min_date,
        style={'position': 'absolute', 'top': '10px', 'right': '10px'}
    ),
    html.Div(id='output-container-date-picker-range'),
    html.Button('Chandelier', id='btn-chandelier', n_clicks=0, style={'background-color': 'lightgray'}, disabled=True, n_clicks_timestamp=0),
    html.Button('Ligne de tendance', id='btn-ligne', n_clicks=0, style={'background-color': 'white'}, disabled=False, n_clicks_timestamp=0),
    

    ])
def create_background():
    return dict(
        title='Graphique',
                title_x=0.02,
                title_y=0.95,
                title_font=dict(size=16, color='white'),
                plot_bgcolor='black',
                paper_bgcolor='black',
                font=dict(color='white'),
                xaxis=dict(
                    showgrid=True, 
                    gridcolor= 'white',
                    gridwidth=0.5,
                    griddash='dot',
                    nticks=20
                ),
                yaxis=dict(
                    title='Prix (USD)',
                    titlefont=dict(size=14, color='white'),
                    tickprefix='$',
                    side='right',
                    showgrid=True,
                    gridcolor='white',
                    gridwidth=0.5,
                    griddash='dot',
                    nticks=20
                )
            )


