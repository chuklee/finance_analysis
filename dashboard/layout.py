import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
from dash import dash_table
import plotly.graph_objs as go
from datetime import date
from data_loader import dataframe_finances

min_date = pd.to_datetime(dataframe_finances['Date']).min().date()
max_date = pd.to_datetime(dataframe_finances['Date']).max().date()
diff_date = (max_date - min_date) / 2 + min_date
def create_layout():
    dataframe_finances['MA20'] = dataframe_finances['AAPL.Close'].rolling(window=20).mean()
    dataframe_finances['SD20'] = dataframe_finances['AAPL.Close'].rolling(window=20).std() 
    dataframe_finances['UpperBand'] = dataframe_finances['MA20'] + (dataframe_finances['SD20']*2)
    dataframe_finances['LowerBand'] = dataframe_finances['MA20'] - (dataframe_finances['SD20']*2)
    return html.Div([
                dcc.Graph(
                    id='graph',
                    figure=go.Figure(data=[go.Candlestick(
    x=dataframe_finances['Date'],
    open=dataframe_finances['AAPL.Open'], high=dataframe_finances['AAPL.High'],
    low=dataframe_finances['AAPL.Low'], close=dataframe_finances['AAPL.Close'],
    increasing_line_color= 'green', decreasing_line_color= 'red',
    showlegend=False

        ),
        *create_bollinger_bands_graph(dataframe_finances)
        ],
            layout= create_background()
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
    html.Div(id='output-container-date-picker-range'
             ),
    html.Button('Chandelier', id='btn-chandelier', n_clicks=0, 
                style={'background-color': 'darkgray',
                        'color': 'white',
                        'position': 'absolute',
                        'top': '10px',
                        'left': '10px'
                        }, 
                disabled=True, n_clicks_timestamp=0),
    html.Button('Ligne de tendance', id='btn-ligne', n_clicks=0, 
                style={'background-color': 'black',
                    'color': 'white',
                    'position': 'absolute',
                    'top': '10px',
                    'left': '160px'
                    }, 
                disabled=False, n_clicks_timestamp=0),
    html.Div(id='stats-table-container')
    ])
def create_background():
    return dict(
        title='Graphique de la valeur de l\'action Apple',
                title_x=0.02,
                title_y=0.93,
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
                ),
                margin=dict(l=20, r=20, t=100, b=50),
                legend=dict(
                    font=dict(
                        size=9
                    )
                )
            )

def create_bollinger_bands_graph(df):
    return [
        go.Scatter(
                    x=df['Date'], y=df['UpperBand'],
                    line=dict(color='gray', width=2, dash='dash'), name='Bande supérieure'
                ),
                go.Scatter(
                    x=df['Date'], y=df['LowerBand'],
                    line=dict(color='gray', width=2, dash='dash'), name='Bande inférieure'  
                ),
                go.Scatter(
                    x=df['Date'], y=df['MA20'],
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
    colors = ['green' if df.loc[i, 'AAPL.Volume'] > df.loc[i - 1, 'AAPL.Volume'] else 'red' for i in range(1, len(df))]
    colors.insert(0, 'green')
    return go.Figure(
            data=[go.Bar(
                x=df['Date'], y=df['AAPL.Volume'],
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
                    gridwidth=0.5,
                    griddash='dot',
                    nticks=20
                ),
                yaxis=dict(
                    title='Volume',
                    showgrid=True,
                    gridcolor='white',
                    gridwidth=0.5,
                    griddash='dot',
                    side='right'
                ),
                margin=dict(l=20, r=20, t=50, b=50)
            )
        )

def create_stats_table(df, start_date, end_date):
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    daily_stats = df.resample('D').agg({
        'AAPL.Open': 'first',
        'AAPL.High': 'max',
        'AAPL.Low': 'min',
        'AAPL.Close': 'last',
        'AAPL.Volume': 'sum'
    })
    daily_stats['Mean'] = daily_stats['AAPL.Close'].mean()
    daily_stats['StdDev'] = daily_stats['AAPL.Close'].std()

    summary_df = pd.DataFrame({
        'Date': daily_stats.index.date,
        'Début': daily_stats['AAPL.Open'],
        'Fin': daily_stats['AAPL.Close'],
        'Min': daily_stats['AAPL.Low'],
        'Max': daily_stats['AAPL.High'],
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
        }
    )





