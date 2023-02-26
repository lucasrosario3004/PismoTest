import pandas as pd

def read_csv(filename, separator):
    return pd.read_csv(filename, sep=separator)
