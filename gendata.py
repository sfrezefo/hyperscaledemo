#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import psycopg2
import time

try:
    conn = psycopg2.connect(user = "citus",
                            password = "Manager12#",
                            host = "postgreshypersg-c.postgres.database.azure.com",
                            port = "5432",
                            database = "citus",
                            sslmode="require")
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")
except:
    print("I am unable to connect to the database")

# The structure of the csv files 1.csv to 10.csv :
# customer_id,event_type,country,browser,device_id,session_id
# 70,scroll,United States,opera,6677,11893

for i in range(10):
    print('==========================open file ============' + str(i+1) + '.csv')
    df = pd.read_csv(str(i+1) + '.csv')
    print(df.head())
    print(df.dtypes)
    df.columns = ['customer_id', 'event_type', 'country', 'browser', 'device_id','session_id']
    
    for index, row in df.iterrows():
        cursor.execute("INSERT INTO events (customer_id, event_type, country, browser, device_id,session_id)"\
           "VALUES (%s,%s,%s,%s,%s,%s)",
            [row['customer_id'],row['event_type'],row['country'],row['browser'],row['device_id'],row['session_id']])
        time.sleep(0.200)
        if(index % 100 == 0):
            print("INSERT INTO events (customer_id, event_type, country, browser, device_id,session_id)"\
                "VALUES (%s,%s,%s,%s,%s,%s)"%(
                row['customer_id'],row['event_type'],row['country'],row['browser'],row['device_id'],row['session_id']))
            conn.commit()
    conn.commit()
conn.commit()
exit()