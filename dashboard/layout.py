import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
from dash import dash_table
import plotly.graph_objs as go
from datetime import date
from data_loader import engine
from data_loader import load_data

def create_layout(dataframe_finances):
    min_date = pd.to_datetime(dataframe_finances['date']).min().date()
    max_date = pd.to_datetime(dataframe_finances['date']).max().date()
    diff_date = (max_date - min_date) / 2 + min_date
    return html.Div([
        html.Div([
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
    ,
        dcc.Store(id='dataframe-store', storage_type='memory', data=dataframe_finances.to_dict('records'))
        ,
         dcc.Dropdown(
            id='company-dropdown',
            options=get_company_options(),
            value=get_company_options()[0]['value'] ,
            style={
                    'width': '200px',
                    'textAlign': 'center',
                    'margin': 'auto',

                    'position': 'absolute',
                    'z-index': '1'

                }
        ),
        dcc.Graph(
            id= 'graph',
            figure=go.Figure(data=[go.Candlestick(
                x= dataframe_finances['date'],
                open= dataframe_finances['open'],
                high= dataframe_finances['high'],
                low= dataframe_finances['low'],
                close= dataframe_finances['close'],
                increasing_line_color= 'green', decreasing_line_color= 'red',
                showlegend=False
            ),
             *create_bollinger_bands_graph(dataframe_finances),
            ],
            layout= create_background(dataframe_finances)
            ),
            style={'height': '780px'}
            ),
            dcc.DatePickerRange(
                id='my-date-picker-range',
                min_date_allowed=min_date,
                max_date_allowed=max_date,
                initial_visible_month=diff_date,
                end_date=max_date,
                start_date=min_date,
                style={'border-color': 'black',
               'background-color': 'black',
               'color': '#333',
               'position': 'absolute',
               'top': '10px',
               'right': '10px'}
            ),
            create_volume_chart(dataframe_finances),
            html.Div(id='output-container-date-picker-range'),
            html.Button('Chandelier', id='btn-chandelier', n_clicks=0, 
                style={'background-color': 'darkgray',
                        'color': 'white',
                        'position': 'absolute',
                        'top': '10px',
                        'left': '220px'
                        }, 
                disabled=True, n_clicks_timestamp=0),
            html.Button('Ligne de tendance', id='btn-ligne', n_clicks=0, 
                        style={'background-color': 'black',
                            'color': 'white',
                            'position': 'absolute',
                            'top': '10px',
                            'left': '370px'
                            }, 
                        disabled=False, n_clicks_timestamp=0),
            html.Div(id='stats-table-container'),



    ])
def get_company_options():
    query = """
        SELECT id, name 
        FROM public.companies
    """
    companies = pd.read_sql(query, con=engine)
    return [{'label': row['name'], 'value': row['id']} for _, row in companies.iterrows()]
def create_background(dataframe_finances):
    min_y = min(dataframe_finances[['open', 'high', 'low', 'close']].min()) 
    max_y = max(dataframe_finances[['open', 'high', 'low', 'close']].max())

    # Ajuster la plage pour avoir un peu de marge 
    margin = (max_y - min_y) * 0.05
    min_y -= margin


    return dict(
        title='Graphique de la valeur de l\'action' + ' ' + dataframe_finances['company_name'].iloc[0],


                title_x=0.02,
                title_y=0.93,
                title_font=dict(size=16, color='white'),
                plot_bgcolor='black',
                paper_bgcolor='black',
                font=dict(color='white'),
                xaxis=dict(
                    showgrid=True, 
                    gridcolor= 'white',
                    gridwidth=0.01,
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
                    gridwidth=0.01,
                    griddash='dot',
                    nticks=20,
                    range=[min_y, max_y]
                ),
                margin=dict(l=20, r=20, t=100, b=50),
                legend=dict(
                    font=dict(
                        size=11
                    )
                )
            )

def create_bollinger_bands_graph(df):
    return [
        go.Scatter(
                    x=df['date'], y=df['UpperBand'],
                    line=dict(color='gray', width=2, dash='dash'), name='Bande supérieure'
                ),
                go.Scatter(
                    x=df['date'], y=df['LowerBand'],
                    line=dict(color='gray', width=2, dash='dash'), name='Bande inférieure'  
                ),
                go.Scatter(
                    x=df['date'], y=df['MA20'],
                    line=dict(color='blue', width=1), name='Moyenne mobile 20 jours'
                )
    ]


def create_volume_chart(df):
    return dcc.Graph(
        id='volume_graph',
        figure=create_volume_figure(df),
        style={'height': '250px'}
    )

def create_volume_figure(df):
    colors = ['green' if df.loc[i, 'volume'] > df.loc[df.index[df.index.get_loc(i)-1], 'volume'] else 'red' 
          for i in df.index[1:]]
    colors.insert(0, 'green' if df.loc[df.index[0], 'volume'] > 0 else 'red')
    y_range = [0, df['volume'].max() * 1.1]
    return go.Figure(
            data=[go.Bar(
                x=df['date'], y=df['volume'],
                marker_color=colors

            )],
            layout=go.Layout(
                title='Volume',
                title_x=0.02,
                plot_bgcolor='black',
                paper_bgcolor='black',
                font=dict(color='white'),
                xaxis=dict(
                    showgrid=True, 
                    gridcolor='white',
                    gridwidth=0.01,
                    griddash='dot',
                    nticks=20
                ),
                yaxis=dict(
                    title='Volume',
                    showgrid=True,
                    gridcolor='white',
                    gridwidth=0.01,
                    griddash='dot',
                    side='right',
                    range=y_range
                ),
                margin=dict(l=20, r=20, t=50, b=50)
            )
        )

def create_stats_table(df: pd.DataFrame, start_date, end_date):
    df = df.assign(date=pd.to_datetime(df['date']))
    df.set_index('date', inplace=True)
    daily_stats = df.resample('D').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })
    '''
    daily_stats = df.groupby(pd.Grouper(key='date', freq='D')).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()'''
    daily_stats['Mean'] = daily_stats['close'].mean()
    daily_stats['StdDev'] = daily_stats['close'].std()

    summary_df = pd.DataFrame({
        'Date': daily_stats.index.date,
        'Début': daily_stats['open'],
        'Fin': daily_stats['close'],
        'Min': daily_stats['low'],
        'Max': daily_stats['high'],
        'Moyenne': daily_stats['Mean'],
        'Ecart type': daily_stats['StdDev']
    })

    return dash_table.DataTable(
        data=summary_df.to_dict('records'),
        columns=[{'name': i, 'id': i} for i in summary_df.columns],
        style_table={'height': '300px', 'overflowY': 'auto'},
        style_cell={
            'backgroundColor': 'black',
            'color': 'white',
            'border': '1px solid grey'
        },
        style_data_conditional=[
            {
                'if': {
                    'filter_query': '{Fin} > {Début}',  
                    'column_id': ['Début', 'Fin', 'Min', 'Max', 'Moyenne', 'Ecart type', 'Date']
                },
                'color': 'green'
            },
            {
                'if': {
                    'filter_query': '{Fin} < {Début}',
                    'column_id': ['Début', 'Fin', 'Min', 'Max', 'Moyenne', 'Ecart type', 'Date'] 
                },
                'color': 'red'
            }
        ],
        style_header={
        'backgroundColor': 'rgb(30, 30, 30)',
        'color': 'white',
        'fontWeight': 'bold'
        }
    )