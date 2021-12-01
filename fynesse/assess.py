from .config import *

import numpy as np
import pandas as pd


def select_top(conn, table, n):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table} LIMIT {n}')
    rows = cur.fetchall()
    return rows


def head(conn, table, n=5):
    rows = select_top(conn, table, n)
    for r in rows:
        print(r)


def tidy_joined_df(data):
    data.rename({0: "price", 1: "date", 2: "type", 3: "postcode", 4: "longitude", 5: "latitude"}, inplace=True,
                axis='columns')
    data['longitude'] = data['longitude'].astype(float)
    data['latitude'] = data['latitude'].astype(float)
    data['log_price'] = np.log(data['price'])
    return data


def query_by_year(conn, year):
    cur = conn.cursor()
    cur.execute(f"""SELECT pp.price as price, pp.`date_of_transfer` as date, pp.`property_type` as type, pp.postcode as postcode, pc.longitude as longitude, pc.lattitude as latitude
                    FROM 
                      (SELECT lattitude, longitude, postcode FROM `postcode_data`) pc
                    INNER JOIN
                      (SELECT price, `date_of_transfer`, postcode, `property_type` FROM `pp_data` WHERE YEAR(`date_of_transfer`) = 2018) pp
                    ON pc.postcode = pp.postcode
                    """)
    rows = cur.fetchall()
    data = pd.DataFrame.from_records(rows)
    return tidy_joined_df(data)


def query_by_postcode(conn, postcode):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM `pp_data` WHERE postcode='{postcode}'")
    rows = cur.fetchall()
    data = pd.DataFrame.from_records(rows)
    return data
