import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.graph_objs as go
from datetime import date



"""
ICI METTRE QUAND TIMESCALEDB SERA INSTALLE
# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)
"""

app = dash.Dash(__name__,  title="Bourse", suppress_callback_exceptions=True) # , external_stylesheets=external_stylesheets)
server = app.server
"""
app.layout = html.Div([
                dcc.Textarea(
                    id='sql-query',
                    value='''
                        SELECT * FROM pg_catalog.pg_tables
                            WHERE schemaname != 'pg_catalog' AND 
                                  schemaname != 'information_schema';
                    ''',
                    style={'width': '100%', 'height': 100},
                    ),
                html.Button('Execute', id='execute-query', n_clicks=0),
                html.Div(id='query-result')
             ])
"""
dataframe_finances = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv')
app.layout = html.Div([
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
        min_date_allowed=date(2016, 1, 1),
        max_date_allowed=date(2017, 1, 1),
        initial_visible_month=date(2016, 8, 5),
        end_date=date(2016, 8, 25)
    ),
    html.Div(id='output-container-date-picker-range')
    ])
@app.callback(
    ddep.Output('output-container-date-picker-range', 'children'),
    ddep.Input('my-date-picker-range', 'start_date'),
    ddep.Input('my-date-picker-range', 'end_date'))
def update_output(start_date, end_date):
    string_prefix = 'You have selected: '
    if start_date is not None:
        start_date_object = date.fromisoformat(start_date)
        start_date_string = start_date_object.strftime('%B %d, %Y')
        string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
    if end_date is not None:
        end_date_object = date.fromisoformat(end_date)
        end_date_string = end_date_object.strftime('%B %d, %Y')
        string_prefix = string_prefix + 'End Date: ' + end_date_string
    if len(string_prefix) == len('You have selected: '):
        return 'Select a date to see it displayed here'
    else:
        return string_prefix
"""
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
"""
if __name__ == '__main__':
    app.run(debug=True)
