

import pandas as pd
import numpy as np
import argparse 
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from pathlib import Path
import os


def transform_data(source_csv, target_dir):
    new_data = pd.read_csv(source_csv)
    #Cleaning
    new_data[['month', 'year']] = new_data.MonthYear.str.split(' ', expand=True)
    new_data['outcome_sex'] = new_data['Sex upon Outcome'].replace('Unknown', np.nan)
    new_data.drop(columns = ['MonthYear', 'Sex upon Outcome'], inplace=True)
    new_data.rename(columns={'Animal ID':'animal_id', 'Name':'name', 'Date of Birth':'dob', 
                             'Animal Type':'animal_type', 'Breed':'breed', 'Color':'color', 
                             'Age upon Outcome':'outcome_age','Outcome Type':'outcome_type', 
                             'Outcome Subtype':'outcome_subtype', 'DateTime':'outtime'}, inplace=True)
    #print(new_data['outcome_type'].value_counts())
    # tabulize: a function that takes in a series, and outputs a tabular format
    #The func takes the input series, ser, and identifies all unique values, assigning them an 
    # id value. Then it outputs the input series in terms of the id values, and also outputs
    # the dictionary mapping series valuesto output values
    #idChar is a string to identify what dat athe id is referencing. ie, outcome id's end in 'o'
    def tabulize(ser, idChar):
        idCol = ser.value_counts(dropna = False).keys()
        dictID = dict(list(zip(idCol, [str(i)+idChar for i in range(len(idCol))])))
        return [dictID[s] for s in ser], dictID


    #get tabular forms of the data
    outcome_id, outcomeDict = tabulize(new_data['outcome_type'], 'o')
    #print(outcomeDict)
    outcomesex_id, outcomesexDict = tabulize(new_data['outcome_sex'], 'ose')
    #print(outcomesexDict)
    outcomesub_id, outcomesubDict = tabulize(new_data['outcome_subtype'], 'osu')

    #Define primary keys for each table. 
    #Rudimentatry: numeric index + corresponding letter of each table
    #note: animal id already exists
    numRows = new_data.shape[0]
    new_data['date_id'] = [str(i)+'d' for i in range(0,numRows)]
    new_data['record_id'] = [str(i)+'r' for i in range(0,numRows)]
    new_data['outcome_id'] = outcome_id
    new_data['outcomesex_id'] = outcomesex_id
    new_data['outcomesub_id'] = outcomesub_id

    #Define what will be in each of the tables
    #Schema: Star Schema -  fact table, animal, date, and outcome dims    
    dates = new_data[['date_id','outtime','month','year']].drop_duplicates(keep='first', inplace=False)
    records = new_data[['record_id', 'animal_id', 'date_id','outcome_id','outcomesex_id','outcomesub_id']].drop_duplicates(keep='first', inplace=False)
    outcomes = pd.DataFrame(data={'outcome_id':outcomeDict.values(), 'outcome_type':outcomeDict.keys()})
    animalInfo = new_data[['animal_id', 'name', 'dob', 'animal_type', 'breed', 'color']].drop_duplicates(keep='first', inplace=False)
    outcomesex = pd.DataFrame(data={'outcomesex_id':outcomesexDict.values(), 'outcome_sex':outcomesexDict.keys()})
    outcomesub = pd.DataFrame(data={'outcomesub_id':outcomesubDict.values(), 'outcome_subtype':outcomesubDict.keys()})
    dTables = [animalInfo, dates, outcomes, outcomesex, outcomesub, records] #Pass tuple of the df's

    Path(target_dir).mkdir(parents=True, exist_ok = True)

    records.to_parquet(target_dir+'/records_fct.parquet')
    dates.to_parquet(target_dir+'/date_dim.parquet')
    outcomes.to_parquet(target_dir+'/outcomes_dim.parquet')
    animalInfo.to_parquet(target_dir+'/animalInfo_dim.parquet')
    outcomesex.to_parquet(target_dir+'/outcomesex_dim.parquet')
    outcomesub.to_parquet(target_dir+'/outcomesub_dim.parquet')
    #return dTables


def load_data(table_file, table_name, key): #Accept tuple of df's
    db_url = os.environ['DB_URL'] # connect to psql server
    conn = create_engine(db_url)

    def insert_on_conflict_nothing(table, conn, keys, data_iter):
        # "key" is the primary key in "conflict_table"
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=[key])
        result = conn.execute(stmt)
        return result.rowcount
    
    pd.read_parquet(table_file).to_sql(table_name, conn, if_exists='append', index=False, method=insert_on_conflict_nothing)
    print(table_name+" loaded.")
    
def load_fact_data(table_file, table_name): #Accept tuple of df's
    db_url = os.environ['DB_URL'] # connect to psql server
    conn = create_engine(db_url)

    pd.read_parquet(table_file).to_sql(table_name, conn, if_exists='replace', index=False)
    print(table_name+" loaded.")