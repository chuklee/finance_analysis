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
        if end_date <= start_date:
            end_date = pd.DataFrame(dataframe_data)['date'].max().date()
            start_date = pd.DataFrame(dataframe_data)['date'].min().date()

        dataframe_finances = pd.DataFrame(dataframe_data)
        # Filter the data based on the selected dates
        filtered_df = dataframe_finances[(dataframe_finances['date'] >= start_date) & (dataframe_finances['date'] <= end_date)]
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
  
def update_date(start_date, end_date, dataframe_data):
    if end_date <= start_date:
            end_date = pd.DataFrame(dataframe_data)['date'].max().date()
            start_date = pd.DataFrame(dataframe_data)['date'].min().date()
    return start_date, end_date

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
        filtered_df = dataframe_finances[(dataframe_finances['date'] >= start_date) & (dataframe_finances['date'] <= end_date)]
        return create_stats_table(filtered_df, start_date, end_date)

    @app.callback(
    ddep.Output('volume_graph', 'figure'),
    [ddep.Input('my-date-picker-range', 'start_date'),
     ddep.Input('my-date-picker-range', 'end_date'),
     ddep.Input('dataframe-store', 'data')]
    )
    def update_volume_graph(start_date, end_date, dataframe_data):
        dataframe_finances = pd.DataFrame(dataframe_data)
        filtered_df = dataframe_finances[(dataframe_finances['date'] >= start_date) & (dataframe_finances['date'] <= end_date)]
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
        diff_date = (max_date - min_date) / 2 + min_date
        return dataframe_finances.to_dict('records'), min_date, max_date, diff_date, min_date, max_date

    @app.callback(
        ddep.Output('date-picker-range', 'min_date_allowed'),
        [ddep.Input('date-picker-range', 'start_date')]
    )
    def update_end_date_min_allowed(start_date):
        if start_date is not None:
            return start_date
        raise dash.exceptions.PreventUpdate
    @app.callback(
        ddep.Output('date-picker-range', 'max_date_allowed'),
        [ddep.Input('date-picker-range', 'end_date')]
    )
    def update_start_date_max_allowed(end_date):
        if end_date is not None:
            return end_date
        raise dash.exceptions.PreventUpdate

    @app.callback( ddep.Output('query-result', 'children'),
               ddep.Input('execute-query', 'n_clicks'),
               ddep.State('sql-query', 'value'),
             )
    def run_query(n_clicks, query):
        if n_clicks > 0:
            try:
                result_df = pd.read_sql_query(query, engine)
                return html.Pre(result_df.to_string())
            except Exception as e:
                return html.Pre(str(e))
        return "Enter a query and press execute."