from .config import *

import urllib.request
import pandas as pd
import zipfile
import osmnx as ox
import geopandas as gpd
from geopandas.tools import sjoin


def create_db_pp(conn):
    cur = conn.cursor()
    cur.execute("""
                CREATE TABLE IF NOT EXISTS `pp_data` (
                  `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
                  `price` int(10) unsigned NOT NULL,
                  `date_of_transfer` date NOT NULL,
                  `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                  `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
                  `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                  `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                  `street` tinytext COLLATE utf8_bin NOT NULL,
                  `locality` tinytext COLLATE utf8_bin NOT NULL,
                  `town_city` tinytext COLLATE utf8_bin NOT NULL,
                  `district` tinytext COLLATE utf8_bin NOT NULL,
                  `county` tinytext COLLATE utf8_bin NOT NULL,
                  `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
                  `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
                  `db_id` bigint(20) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
    """)


def download_pp(conn, year, part):
    name = 'pp-' + str(year) + '-part' + str(part) + '.csv'
    urllib.request.urlretrieve('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/' + name,
                               name)
    data = pd.read_csv(name, header=None)
    data.to_csv(name, index=None, header=None)
    cur = conn.cursor()
    cur.execute(
        f"LOAD DATA LOCAL INFILE '{name}' INTO TABLE `pp_data` FIELDS TERMINATED BY ',' LINES STARTING BY '' TERMINATED BY '\n'")
    conn.commit()
    cur.close()


def data_pp(conn):
    for year in range(1995, 2022):
        download_pp(conn, year, 1)
        download_pp(conn, year, 2)


def create_index_pp(conn):
    cur = conn.cursor()
    cur.execute("""
                CREATE INDEX `pp.postcode` USING HASH ON `pp_data` (postcode);
                CREATE INDEX `pp.date` USING HASH ON `pp_data` (date_of_transfer);
    """)


def create_db_postcode(conn):
    cur = conn.cursor()
    cur.execute("""
                CREATE TABLE IF NOT EXISTS `pp_data` (
                  `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
                  `price` int(10) unsigned NOT NULL,
                  `date_of_transfer` date NOT NULL,
                  `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                  `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
                  `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                  `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
                  `street` tinytext COLLATE utf8_bin NOT NULL,
                  `locality` tinytext COLLATE utf8_bin NOT NULL,
                  `town_city` tinytext COLLATE utf8_bin NOT NULL,
                  `district` tinytext COLLATE utf8_bin NOT NULL,
                  `county` tinytext COLLATE utf8_bin NOT NULL,
                  `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
                  `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
                  `db_id` bigint(20) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
    """)


def data_postcode(conn):
    urllib.request.urlretrieve('https://www.getthedata.com/downloads/open_postcode_geo.csv.zip',
                               'open_postcode_geo.csv.zip')
    z = zipfile.ZipFile("open_postcode_geo.csv.zip")
    data = pd.read_csv(z.open("open_postcode_geo.csv"))
    data.to_csv("open_postcode_geo.csv", index=True)
    cur = conn.cursor()
    cur.execute(
        f"LOAD DATA LOCAL INFILE 'open_postcode_geo.csv' INTO TABLE `postcode_data` FIELDS TERMINATED BY ',' LINES STARTING BY '' TERMINATED BY '\n'")
    conn.commit()
    cur.close()


def create_index_postcode(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX `po.postcode` USING HASH ON `postcode_data` (postcode);")


def create_db_prices_coord(conn):
    cur = conn.cursor()
    cur.execute("""
                CREATE TABLE IF NOT EXISTS `prices_coordinates_data` (
                  `price` int(10) unsigned NOT NULL,
                  `date_of_transfer` date NOT NULL,
                  `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
                  `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
                  `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
                  `locality` tinytext COLLATE utf8_bin NOT NULL,
                  `town_city` tinytext COLLATE utf8_bin NOT NULL,
                  `district` tinytext COLLATE utf8_bin NOT NULL,
                  `county` tinytext COLLATE utf8_bin NOT NULL,
                  `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
                  `lattitude` decimal(11,8) NOT NULL,
                  `longitude` decimal(10,8) NOT NULL,
                  `db_id` bigint(20) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
    """)


def join_pp_postcode(conn, low_long, high_long, low_lat, high_lat, year, property_type):
    cur = conn.cursor()
    cur.execute(f"""
              SELECT pp.price as price, pp.`date_of_transfer` as date, pp.`property_type` as type, pp.postcode as postcode, pc.longitude as longitude, pc.lattitude as latitude
              FROM 
                (SELECT lattitude, longitude, postcode FROM `postcode_data` WHERE longitude>{low_long} AND longitude<{high_long} AND lattitude>{low_lat} AND lattitude<{high_lat}) pc
              INNER JOIN
                (SELECT price, `date_of_transfer`, postcode, `property_type` FROM `pp_data` WHERE `property_type` = '{property_type}' AND YEAR(`date_of_transfer`) = {year}) pp
              ON pc.postcode = pp.postcode
              """)
    rows = cur.fetchall()
    data = pd.DataFrame.from_records(rows)
    return data


def join_pp_postcode_other(low_long, high_long, low_lat, high_lat, year, conn):
    cur = conn.cursor()
    cur.execute(f"""
              SELECT pp.price as price, pp.`date_of_transfer` as date, pp.`property_type` as type, pp.postcode as postcode, pc.longitude as longitude, pc.lattitude as latitude
              FROM 
                (SELECT lattitude, longitude, postcode FROM `postcode_data` WHERE longitude>{low_long} AND longitude<{high_long} AND lattitude>{low_lat} AND lattitude<{high_lat}) pc
              INNER JOIN
                (SELECT price, `date_of_transfer`, postcode, `property_type` FROM `pp_data` WHERE YEAR(`date_of_transfer`) = {year}) pp
              ON pc.postcode = pp.postcode
              """)
    rows = cur.fetchall()
    data = pd.DataFrame.from_records(rows)
    return data


def joined_to_db(data, conn):
    data.to_csv('joined_pp_cord.csv', index=False, header=False)
    cur = conn.cursor()
    cur.execute(
        f"LOAD DATA LOCAL INFILE 'joined_pp_cord.csv' INTO TABLE `prices_coordinates_data` FIELDS TERMINATED BY ',' LINES STARTING BY '' TERMINATED BY '\n'")
    conn.commit()
    cur.close()


def to_gdf(data):
    geometry = gpd.points_from_xy(data.longitude, data.latitude)
    gp_data = gpd.GeoDataFrame(data, geometry=geometry)
    gp_data.crs = "EPSG:4326"
    gp_data['geometry'] = gp_data['geometry'].buffer(0.005)
    gp_data.reset_index(inplace=True)
    return gp_data


def add_pois(data, t, box_size, latitude, longitude):
    gp_data = to_gdf(data)
    for tag in t:
        tags = {tag: True}
        col_name = "count_" + tag
        pois = ox.geometries_from_bbox(latitude + box_size, latitude - box_size, longitude + box_size,
                                       longitude - box_size, tags)
        if len(pois) == 0:
            continue
        pois.reset_index(inplace=True)
        pois.set_index('osmid', inplace=True)
        gp_data[col_name] = sjoin(gp_data, pois, how='left').groupby(['index']).count()['price']
    return gp_data
