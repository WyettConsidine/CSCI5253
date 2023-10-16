import pandas as pd
import numpy as np
from pipeline import transform_data

def read_data():
    df = pd.read_csv('C:/Users/wyett/OneDrive/Documents/CSCI5253/DockerThings/shelter1000.csv')
    #print(df)
    
    return df


if __name__ == "__main__": 
    print("start tester routine")
    df = read_data()
    tables = transform_data(df)
    #print(tables)
    print('(complete)')
