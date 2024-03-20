import pandas as pd


def load_data():
    return pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv')

dataframe_finances = load_data()