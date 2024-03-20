import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.graph_objs as go
from datetime import date
from data_loader import dataframe_finances
from layout import create_layout

def callbacks(app):
    @app.callback(
        ddep.Output('graph', 'figure'),
        [ddep.Input('my-date-picker-range', 'start_date'),
        ddep.Input('my-date-picker-range', 'end_date'),
         ddep.Input('btn-chandelier', 'n_clicks'),
        ddep.Input('btn-ligne', 'n_clicks')]
    )
    def update_graph(start_date, end_date,chandelier_clicks, line_clicks):
        # Déterminer le type de graphique en fonction du nombre de clics sur chaque bouton
        graph_type = 'chandelier' if chandelier_clicks > line_clicks else 'line'

        # Filtrer les données en fonction des dates sélectionnées
        filtered_df = dataframe_finances[(dataframe_finances['Date'] >= start_date) & (dataframe_finances['Date'] <= end_date)]

        # Créer le graphique en fonction du type sélectionné
        if graph_type == 'chandelier':
            candlestick = go.Candlestick(
                x=filtered_df['Date'],
                open=filtered_df['AAPL.Open'],
                high=filtered_df['AAPL.High'],
                low=filtered_df['AAPL.Low'],
                close=filtered_df['AAPL.Close'],
                increasing_line_color='green',
                decreasing_line_color='red'
            )
            figure = go.Figure(data=[candlestick])
        else:
            line = go.Scatter(
                x=filtered_df['Date'],
                y=filtered_df['AAPL.Close'],
                mode='lines',
                name='AAPL.Close'
            )
            figure = go.Figure(data=[line])

        return figure
        
        
    
