import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.graph_objs as go
from datetime import date
from data_loader import dataframe_finances
from layout import create_bollinger_bands_graph, create_background, create_stats_table, create_volume_figure

def build_candlestick_graph(filtered_df):
    return go.Figure(data=[go.Candlestick(
                x=filtered_df['Date'],
                open=filtered_df['AAPL.Open'],
                high=filtered_df['AAPL.High'],
                low=filtered_df['AAPL.Low'],
                close=filtered_df['AAPL.Close'],
                increasing_line_color='green',
                decreasing_line_color='red',
                showlegend=False

            ),
            *create_bollinger_bands_graph(filtered_df)],
            layout= create_background()
            )
def build_line_graph(filtered_df):
    return go.Figure(data=[go.Scatter(
                x=filtered_df['Date'],
                y=filtered_df['AAPL.Close'],
                mode='lines',
                line=dict(color='green'),
                showlegend= False

                
            ),
            *create_bollinger_bands_graph(filtered_df)],
            layout= create_background()
            )
def callbacks(app):
    @app.callback(
    ddep.Output('graph', 'figure'),
    [ddep.Input('my-date-picker-range', 'start_date'),
     ddep.Input('my-date-picker-range', 'end_date'),
    ddep.Input('btn-chandelier', 'n_clicks_timestamp'),
     ddep.Input('btn-ligne', 'n_clicks_timestamp')]
    )
    def update_graph(start_date, end_date,
                 btn_chandelier_timestamp, btn_ligne_timestamp):

        # Filter the data based on the selected dates
        filtered_df = dataframe_finances[(dataframe_finances['Date'] >= start_date) & (dataframe_finances['Date'] <= end_date)]
        if btn_ligne_timestamp > btn_chandelier_timestamp:
            graph_type = 'ligne'
        else:
            graph_type = 'chandelier'

        # Create the graph based on the selected type
        if graph_type == 'chandelier':

            figure = build_candlestick_graph(filtered_df)
            figure.update_layout(xaxis_rangeslider_visible=False)


        else:
            figure = build_line_graph(filtered_df)
        
        return figure
    @app.callback(
    [ddep.Output('btn-chandelier', 'style'),
     ddep.Output('btn-ligne', 'style'),
     ddep.Output('btn-chandelier', 'disabled'),
     ddep.Output('btn-ligne', 'disabled')],
    [ddep.Input('btn-chandelier', 'n_clicks'),
     ddep.Input('btn-ligne', 'n_clicks')]

    )
    def update_buttons(btn_chandelier, btn_ligne):
        changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
        
        if 'btn-chandelier' in changed_id:
            return [{'background-color': 'lightgray', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '10px'}, 
                    {'background-color': 'black', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '160px'}, True, False]
        elif 'btn-ligne' in changed_id:
            return [{'background-color': 'black', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '10px'}, 
                    {'background-color': 'lightgray', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '160px'}, False, True]
        else:
            return [{'background-color': 'lightgray', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '10px'}, 
                    {'background-color': 'black', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '160px'}, True, False]
    @app.callback(
    ddep.Output('stats-table-container', 'children'),
    [ddep.Input('my-date-picker-range', 'start_date'),
     ddep.Input('my-date-picker-range', 'end_date')]
    )
    def update_stats_table(start_date, end_date):
        filtered_df = dataframe_finances[(dataframe_finances['Date'] >= start_date) & (dataframe_finances['Date'] <= end_date)]
        return create_stats_table(filtered_df, start_date, end_date)

    @app.callback(
    ddep.Output('volume_graph', 'figure'),
    [ddep.Input('my-date-picker-range', 'start_date'),
     ddep.Input('my-date-picker-range', 'end_date')]
    )
    def update_volume_graph(start_date, end_date):
        filtered_df = dataframe_finances[(dataframe_finances['Date'] >= start_date) & (dataframe_finances['Date'] <= end_date)]
        return create_volume_figure(filtered_df)







    









        
        
    
