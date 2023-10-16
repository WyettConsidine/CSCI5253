#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import argparse 
from sqlalchemy import create_engine, text

def extract_data(source):
    return pd.read_csv(source)  

def transform_data(data):
    new_data = data.copy()


    #Cleaning
    new_data[['month', 'year']] = new_data.MonthYear.str.split(' ', expand=True)
    new_data['outcome_sex'] = new_data['Sex upon Outcome'].replace('Unknown', np.nan)
    new_data.drop(columns = ['MonthYear', 'Sex upon Outcome'], inplace=True)
    new_data.rename(columns={'Animal ID':'animal_id', 'Name':'name', 'Date of Birth':'dob', 
                             'Animal Type':'animal_type', 'Breed':'breed', 'Color':'color', 
                             'Age upon Outcome':'outcome_age','Outcome Type':'outcome_type', 
                             'Outcome Subtype':'outcome_subtype', 'DateTime':'outtime'}, inplace=True)
    
    # table 1: OutcomeID:
    def tabulize(ser, idChar):
        idCol = ser.value_counts(dropna = False).keys()
        dictID = dict(list(zip(idCol, [str(i)+idChar for i in range(len(idCol))])))
        return [dictID[s] for s in ser], dictID


    outcome_id, outcomeDict = tabulize(new_data['outcome_type'])

    print(outcome_id)
    print(list(outcomeDict))
            

    #Define primary keys for each table. 
    #Rudimentatry: numeric index + corresponding letter of each table
    #note: animal id already exists
    new_data['date_id'] = [str(i)+'d' for i in range(0,1000)]
    new_data['record_id'] = [str(i)+'r' for i in range(0,1000)]
    new_data['outcome_id'] = [str(i)+'O' for i in range(0,1000)]
    new_data['outcomesex_id'] = [str(i)+'s' for i in range(0,1000)]
    #Define what will be in each of the tables
    #Schema: Star Schema -  fact table, animal, date, and outcome dims    
    dates = new_data[['date_id','outtime','month','year']].drop_duplicates(keep='first', inplace=False)
    records = new_data[['record_id', 'animal_id', 'date_id','outcome_id','outcomesex_id']].drop_duplicates(keep='first', inplace=False)
    outcomes = new_data[['outcome_id','outcome_type', 'outcome_subtype']].drop_duplicates(keep='first', inplace=False)
    animalInfo = new_data[['animal_id', 'name', 'dob', 'animal_type', 'breed', 'color']].drop_duplicates(keep='first', inplace=False)
    outcomesex = new_data[['outcomesex_id', 'outcome_sex']].drop_duplicates(keep='first', inplace=False)
    dTables = [animalInfo, dates, outcomes, outcomesex, records] #Pass tuple of the 4 df's

    return dTables

def load_data(data): #Accept tuple of 4 df's
    print("tester")
    db_url = 'postgresql+psycopg2://wyett:password@db:5432/shelter' # connect to psql server
    conn = create_engine(db_url)
    tableNames = ["animalinfo_dim", "date_dim", "outcomes_dim", "outcomesex_dim", "animalrecords_fct",] 
    #This will be a for loop that adds data to each table. 
    #Only adding to animal dimension table for debugging
    for i in range(0,len(data)):
        #print(data[0].head())
        data[i].to_sql(tableNames[i], conn, if_exists='append', index=False)



if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    #parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Starting...")
    df = extract_data(args.source)
    print(df.head())
    new_df = transform_data(df)
    load_data(new_df)
    print("Complete")

#make a separte table for cats, dogs, bird other
#separate tables for anim type, breed, color

# consider having outcomes have forgien key to animals
