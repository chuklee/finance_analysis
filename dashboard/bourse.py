import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import pandas as pd
import sqlalchemy
import plotly.graph_objs as go
from datetime import date
from layout import create_layout
from data_loader import load_data
from callback import callbacks  



# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__,  title="Bourse", suppress_callback_exceptions=True) # , external_stylesheets=external_stylesheets)
server = app.server
app.layout = create_layout(load_data('1'))
callbacks(app)

if __name__ == '__main__':
    app.run(debug=True, dev_tools_hot_reload=500)