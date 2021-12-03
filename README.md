# Fynesse ADS


## access.py

1. CREATE TABLE IF NOT EXISTS 
   - `create_table_pp(conn)` : _pp_data_
   - `create_table_postcode(conn)` : _postcode_data_
   - `create_table_prices_coord(conn)` : _prices_coordinates_data_



2. DROP IF EXISTS
   - `drop_table(conn, table_name)`



3. LOAD DATA INTO TABLE
   - `data_pp(conn)` : UK Priced Paid data (1995-2021) into _pp_data_
   - `data_postcode(conn)` :  postcode data into _postcode_data_
   - `data_joined(record, conn)` : _pp_data_ and _postcode_data_ inner-joined on postcode into _price_coordinates_data_
     - obtain record by calling `join_pp_postcode(conn, north, south, east, west, start_date, end_date)` with specific region and time period
    


4. CREATE INDEX
   - `create_index_pp(conn)`: hash on _postcode_, _date-of-transfer_
   - `create_index_postcode(conn)`: hash on _postcode_


5. OpenStreetMap data
   - `add_pois(df, tag_list, col_name_list, dist, north, south, east, west)`: for each tag, add a column containing the count of the pois within _dist_ of each point
     - get all pois inside the bounding box using  _osmnx.geometries_from_bbox_
     - buffers each point by _dist_ using `to_gdf(data, distance)`
     - get count for individual points by using _geopandas.sjoin_



## assess.py
1. `head(conn, table_name, n)`: print first _n_ rows of the _table_name_
   - to check whether the schema matches the data
   - to check whether the data has been correctly parsed and loaded
   - to check how NULL values, if it exists, are represented


2. Queries
   - `query_by_postcode(conn, postcode)`: return dataframe containing rows filtered from _pp_data_
   - `query_by_date(conn, start_date, end_date)`: return record containing the inner-joined data satisfying the specified date range  
   - `query_joined_year(conn, north, south, east, west, year)`: return record containing the inner-joined data satisfying the specified bbox and year
   - `query_joined_year_type(conn, north, south, east, west, year, property_type)`: return record containing the inner-joined data satisfying the specified bbox, year and property_type



3. `record_to_df(record)`/`short_record_to_df(record)`: add structure and tidy up the output of queries involving joins
   - change data types of longitude/latitude for future use
   - remove unused columns and add _log_price_
   - give informative names to the columns
   

4. `pca_df(data, n)`: return dataframe obtained by standardising the features of _data_ and fitting with _sklearn.decomposition.PCA(n)_
   

## address.py
1. `choose_training_data(conn, latitude, longitude, year, property_type, box_size)`

2. `train_model(gp_data)`

3. `predict_price(conn, latitude, longitude, year, property_type)`


### To be tested
- size of the training data

- cannot make prediction outside the range - training data is defined to be of the same year 

- the bounds of the accepted inputs : raises an error if the size of the query is 0, warning if lower than 1000