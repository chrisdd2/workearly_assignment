from datetime import datetime

import pandas as pd
import pymysql as mysql
from tqdm import tqdm
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl



def import_data(db,sql_file):
    """ Executes the sql_file sql statements to the database """
    with open(sql_file) as f:
        sql = f.read()
    with db.cursor() as cur:
        for line in sql.split(';'):
            cmd = line.strip()
            cur.execute(cmd)
            print(cur.fetchone())


def extract_to_csv(db, table, database):
    """ Extracts database.table into table.csv file"""
    with db.cursor() as cur:
        cur.execute(f'use {database}')
        # cur.execute(f'select * from {table}')
        cur.execute(f'select * from {table} where year(date) between 2016 and 2019')
        df = pd.DataFrame(data=tqdm(cur, desc=table), columns=[c[0] for c in cur.description])
    df.to_csv(table + '.csv', index=False)


if __name__ == '__main__':
    table = 'finance_liquor_sales'
    database = 'liquorSales'
    sql_file ='finance_liquor_sales.sql'

    # db = mysql.connect(host='localhost', port=3306, user='root', password='qwertygr123')
    # import_data(db,sql_file)
    # extract_to_csv(db, table, database)
    # db.close()

    # read csv
    df = pd.read_csv(table + '.csv')

    fig,(a1,a2) = plt.subplots(2,1)
    # fix zip codes, 50320.0, 50320
    df['zip_code'] = pd.to_numeric(df['zip_code'])

    # bottles sold per zip code
    bottles_df = df[['item_number', 'zip_code', 'bottles_sold']].groupby(
        by=['item_number', 'zip_code']).sum().reset_index().sort_values(by='bottles_sold', ascending=False)

    a1.scatter(x=bottles_df['zip_code'], y=bottles_df['bottles_sold'], c=np.random.rand(len(bottles_df)), alpha=1)
    a1.set_xlabel('Zip Code')
    a1.set_ylabel('Bottles Sold')
    a1.set_title('Bottles Sold')

    # calculate percentage of sales_dollars per store
    col = 'sale_dollars'
    sales_df = df.copy()
    sales_df['date'] = pd.to_datetime(sales_df['date'])
    perc_df = sales_df[['store_number', col]].groupby(by=['store_number']).sum().reset_index()
    perc_df['percentage'] = (perc_df[col] / perc_df[col].sum()) * 100
    perc_df= perc_df.sort_values(by='percentage', ascending=True).reset_index(drop=True)

    perc_df['store_number'] = perc_df['store_number'].astype(str)
    limit = 20
    perc_df = perc_df.iloc[-limit:,]

    # color map cause why not
    viridis = mpl.colormaps['viridis'].resampled(100)
    # barchar for sales per store
    barh = a2.barh(perc_df['store_number'],perc_df['percentage'],align='center',color=viridis(np.random.rand(limit)))
    a2.set_title('Sales percentage per Store')
    a2.set_ylabel('Store Number')
    a2.set_xlabel('Sales percentage')
    # labels on bars
    a2.bar_label(barh,fmt='%.2f%%')
    plt.show()

