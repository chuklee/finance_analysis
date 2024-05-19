import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.graph_objs as go
from datetime import date, timedelta
from layout import create_bollinger_bands_graph, create_background, create_stats_table, create_volume_figure, engine
from data_loader import load_data

def build_candlestick_graph(filtered_df):
    min_y = min(filtered_df[['open', 'high', 'low', 'close']].min()) 
    max_y = max(filtered_df[['open', 'high', 'low', 'close']].max())

    # Ajuster la plage pour avoir un peu de marge 
    margin = (max_y - min_y) * 0.05
    min_y -= margin
    max_y += margin
    return go.Figure(data=[go.Candlestick(
                x=filtered_df['date'],
                open=filtered_df['open'],
                high=filtered_df['high'],
                low=filtered_df['low'],
                close=filtered_df['close'],
                increasing_line_color='green',
                decreasing_line_color='red',
                showlegend=False

            ),
            *create_bollinger_bands_graph(filtered_df)],
            layout= create_background(filtered_df)
            )
def build_line_graph(filtered_df):
    return go.Figure(data=[go.Scatter(
                x=filtered_df['date'],
                y=filtered_df['close'],
                mode='lines',
                line=dict(color='green'),
                showlegend= False

                
            ),
            *create_bollinger_bands_graph(filtered_df)],
            layout= create_background(filtered_df)
            )
def update_graph(start_date, end_date,
                 btn_chandelier_timestamp, btn_ligne_timestamp, dataframe_data):
        dataframe_finances = pd.DataFrame(dataframe_data)
        if dataframe_finances.get('date') is None:
            if btn_ligne_timestamp > btn_chandelier_timestamp:
                return go.Figure(data=[go.Scatter()])
            else:
                return go.Figure(data=[go.Candlestick()])

        filtered_df = dataframe_finances[(pd.to_datetime(dataframe_finances['date']) >= start_date) & (pd.to_datetime(dataframe_finances['date']) <= end_date)]

        if filtered_df['volume'].isnull().all():
            if btn_ligne_timestamp > btn_chandelier_timestamp:
                return go.Figure(data=[go.Scatter()])
            else:
                return go.Figure(data=[go.Candlestick()])
        # Filter the data based on the selected dates
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
  


def callbacks(app):
    

    @app.callback(
    ddep.Output('graph', 'figure'),
    [ddep.Input('my-date-picker-range', 'start_date'),
     ddep.Input('my-date-picker-range', 'end_date'),
    ddep.Input('btn-chandelier', 'n_clicks_timestamp'),
     ddep.Input('btn-ligne', 'n_clicks_timestamp'),
     ddep.Input('dataframe-store', 'data')]
    )
    def update_time(start_date, end_date,
                 btn_chandelier_timestamp, btn_ligne_timestamp, dataframe_data):
        return update_graph(start_date, end_date, btn_chandelier_timestamp, btn_ligne_timestamp, dataframe_data)
    

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
                    'left': '220px'}, 
                    {'background-color': 'black', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '370px'}, True, False]
        elif 'btn-ligne' in changed_id:
            return [{'background-color': 'black', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '220px'}, 
                    {'background-color': 'lightgray', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '370px'}, False, True]
        else:
            return [{'background-color': 'lightgray', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '220px'}, 
                    {'background-color': 'black', 'color': 'white', 'position': 'absolute',
                    'top': '10px',
                    'left': '370px'}, True, False]
    @app.callback(
    ddep.Output('stats-table-container', 'children'),
    [ddep.Input('my-date-picker-range', 'start_date'),
     ddep.Input('my-date-picker-range', 'end_date'),
     ddep.Input('dataframe-store', 'data')]
    )
    def update_stats_table(start_date, end_date, dataframe_data):
        dataframe_finances = pd.DataFrame(dataframe_data)
        if dataframe_finances.get('date') is None:
            return html.Div()
        filtered_df = dataframe_finances[(pd.to_datetime(dataframe_finances['date']) >= start_date) & (pd.to_datetime(dataframe_finances['date']) <= end_date)]
        if filtered_df['volume'].isnull().all() :
            return html.Div()
        return create_stats_table(filtered_df)

    @app.callback(
    ddep.Output('volume_graph', 'figure'),
    [ddep.Input('my-date-picker-range', 'start_date'),
     ddep.Input('my-date-picker-range', 'end_date'),
     ddep.Input('dataframe-store', 'data')]
    )
    def update_volume_graph(start_date, end_date, dataframe_data):
        dataframe_finances = pd.DataFrame(dataframe_data)
        if dataframe_finances.get('date') is None:
            return go.Figure()
        filtered_df = dataframe_finances[(pd.to_datetime(dataframe_finances['date']) >= start_date) & (pd.to_datetime(dataframe_finances['date']) <= end_date)]
        if filtered_df['volume'].isnull().all():
            return go.Figure()
        return create_volume_figure(filtered_df)
    @app.callback(
        [ddep.Output('dataframe-store', 'data'),
         ddep.Output('my-date-picker-range', 'start_date'),
        ddep.Output('my-date-picker-range', 'end_date'),
        ddep.Output('my-date-picker-range', 'initial_visible_month'),
        ddep.Output('my-date-picker-range', 'min_date_allowed'),
        ddep.Output('my-date-picker-range', 'max_date_allowed')],
        ddep.Input('company-dropdown', 'value')
        )
    def update_dataframe(cid):
        dataframe_finances = load_data(str(cid))
        min_date = pd.to_datetime(dataframe_finances['date'].min())
        max_date = pd.to_datetime(dataframe_finances['date'].max())
        return dataframe_finances.to_dict('records'), min_date, max_date, min_date, min_date, max_date