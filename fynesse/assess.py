from .config import *

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


def select_top(conn, table, n):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table} LIMIT {n}')
    rows = cur.fetchall()
    return rows


def head(conn, table, n=5):
    rows = select_top(conn, table, n)
    for r in rows:
        print(r)


def tidy_joined_record(record):
    data = pd.DataFrame.from_records(record)
    data.rename({0: "price", 1: "date", 2: "postcode", 3: "type", 11: "latitude", 12: "longitude"}, inplace=True,
                axis='columns')
    data = data[["price", "date", "postcode", "type", "latitude", "longitude"]]
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
                      (SELECT price, `date_of_transfer`, postcode, `property_type` FROM `pp_data` WHERE YEAR(`date_of_transfer`) = {year}) pp
                    ON pc.postcode = pp.postcode
                    """)
    rows = cur.fetchall()
    data = tidy_joined_record(rows)
    return data


def query_by_postcode(conn, postcode):
    cur = conn.cursor()
    cur.execute(f"""SELECT * 
                    FROM `pp_data` 
                    WHERE postcode='{postcode}'
                    """)
    rows = cur.fetchall()
    data = pd.DataFrame.from_records(rows)
    data = data.rename({0: 'tid', 1: 'price', 2: 'date', 3: 'postcode', 4: 'type'}, axis='columns')
    data = data[["tid", "price", "date", "postcode", "type"]]
    data['log_price'] = np.log(data['price'])
    return data


def pca_data(data, n, filter=False):
    if filter:
        data = data[data['price'] < data['price'].quantile(0.99)]
    x = StandardScaler().fit_transform(data)
    pca = PCA(n)
    x_pca = pca.fit_transform(x)
    df = pd.DataFrame(data=x_pca)
    return df
