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


def record_to_df(record):
    data = pd.DataFrame.from_records(record)
    data.rename({0: "price", 1: "date", 2: "postcode", 3: "type", 12: "latitude", 11: "longitude"}, inplace=True,
                axis='columns')
    data = data[["price", "date", "postcode", "type", "latitude", "longitude"]]
    data['longitude'] = data['longitude'].astype(float)
    data['latitude'] = data['latitude'].astype(float)
    data['log_price'] = np.log(data['price'])
    return data


def short_record_to_df(record):
    data = pd.DataFrame.from_records(record)
    data.rename({0: "price", 1: "date", 2: "postcode", 3: "type", 5: "latitude", 4: "longitude"}, inplace=True,
                axis='columns')
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
    data = record_to_df(rows)
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


def join_by_year(conn, north, south, east, west, year):
    cur = conn.cursor()
    cur.execute(f"""
              SELECT pp.price, pp.`date_of_transfer`, pp.postcode, pp.`property_type`, pc.longitude, pc.lattitude
              FROM 
                (SELECT lattitude, longitude, postcode, country FROM `postcode_data` WHERE longitude>{west} AND longitude<{east} AND lattitude>{south} AND lattitude<{north}) pc
              INNER JOIN
                (SELECT price, `date_of_transfer`, postcode, `property_type` FROM `pp_data` WHERE YEAR(`date_of_transfer`) = {year}) pp 
              ON pc.postcode = pp.postcode
              """)
    rows = cur.fetchall()
    return rows


def join_by_year_type(conn, north, south, east, west, year, property_type):
    cur = conn.cursor()
    cur.execute(f"""
              SELECT pp.price, pp.`date_of_transfer`, pp.postcode, pp.`property_type`, pc.longitude, pc.lattitude
              FROM 
                (SELECT lattitude, longitude, postcode, country FROM `postcode_data` WHERE longitude>{west} AND longitude<{east} AND lattitude>{south} AND lattitude<{north}) pc
              INNER JOIN
                (SELECT price, `date_of_transfer`, postcode, `property_type` FROM `pp_data` WHERE `property_type`='{property_type}' AND YEAR(`date_of_transfer`) = {year}) pp 
              ON pc.postcode = pp.postcode
              """)
    rows = cur.fetchall()
    return rows


def pca_df(data, n, filter_price=False):
    if filter_price:
        data = data[data['price'] < data['price'].quantile(0.99)]
    x = StandardScaler().fit_transform(data)
    pca = PCA(n)
    x_pca = pca.fit_transform(x)
    df = pd.DataFrame(data=x_pca)
    return df
