from .config import *

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


def head(conn, table, n=5):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table} LIMIT {n}')
    rows = cur.fetchall()
    for r in rows:
        print(r)


def joined_record_to_df(record):
    data = pd.DataFrame.from_records(record)
    data.rename({0: "price", 1: "date", 2: "postcode", 3: "type", 12: "latitude", 11: "longitude"}, inplace=True,
                axis='columns')
    data = data[["price", "date", "postcode", "type", "latitude", "longitude"]]
    data['longitude'] = data['longitude'].astype(float)
    data['latitude'] = data['latitude'].astype(float)
    data['log_price'] = np.log(data['price'])
    return data


def query_by_date(conn, start_date, end_date):
    cur = conn.cursor()
    cur.execute(f"""SELECT pp.price as price, pp.`date_of_transfer` as date, pp.`property_type` as type, pp.postcode as postcode, pc.longitude as longitude, pc.lattitude as latitude
                    FROM 
                      (SELECT lattitude, longitude, postcode FROM `postcode_data`) pc
                    INNER JOIN
                      (SELECT price, `date_of_transfer`, postcode, `property_type` FROM `pp_data` WHERE DATE(`date_of_transfer`) BETWEEN '{start_date}' AND '{end_date}') pp
                    ON pc.postcode = pp.postcode
                    """)
    rows = cur.fetchall()
    data = joined_record_to_df(rows)
    return data


def query_by_postcode(conn, postcode):
    cur = conn.cursor()
    cur.execute(f"""SELECT * 
                    FROM `pp_data` 
                    WHERE postcode='{postcode}'
                    """)
    rows = cur.fetchall()
    data = pd.DataFrame.from_records(rows)
    data.rename({0: 'tid', 1: 'price', 2: 'date', 3: 'postcode', 4: 'type'}, axis='columns', inplace=True)
    data = data[["tid", "price", "date", "postcode", "type"]]
    data['log_price'] = np.log(data['price'])
    return data


def pca_df(data, n, filter_price=False):
    if filter_price:
        data = data[data['price'] < data['price'].quantile(0.99)]
    x = StandardScaler().fit_transform(data)
    pca = PCA(n)
    x_pca = pca.fit_transform(x)
    df = pd.DataFrame(data=x_pca)
    return df


def tidy_dataframe(record):
    data = pd.DataFrame.from_records(record)
    data.rename({0: "price", 1: "date", 2: "postcode", 3: "type", 4: "longitude", 5: "latitude"}, inplace=True,
                     axis='columns')
    data['longitude'] = data['longitude'].astype(float)
    data['latitude'] = data['latitude'].astype(float)
    data['log_price'] = np.log(data['price'])
    return data
