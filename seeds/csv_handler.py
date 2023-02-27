import pandas as pd

def read_csv(filename, separator):
    return pd.read_csv(filename, sep=separator)

def write_csv(filename, separator, df):
    print('writing to '+filename)
    return df.to_csv(filename, index=False, sep=separator)