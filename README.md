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
   - `data_joined(record, conn)` : _pp_data_ and _postcode_data_ inner-joined on _postcode_ into _price_coordinates_data_
     - record to input can be obtained by calling `join_pp_postcode(conn, north, south, east, west, start_date, end_date)` with specific bbox and time period
    


4. CREATE INDEX
   - `create_index_pp(conn)`: hash on _postcode_ and _date-of-transfer_
   - `create_index_postcode(conn)`: hash on _postcode_


5. OpenStreetMap data
   - `add_pois(df, tag_list, col_name_list, dist, north, south, east, west)`: for each tag, add a column containing the count of the pois within _dist_ of each point
     - get all pois inside the bbox using  _osmnx.geometries_from_bbox_
     - buffer each point by _dist_
     - get count for individual points by using _geopandas.sjoin_



## assess.py
1. `head(conn, table_name, n)`: print first _n_ rows of the _table_name_
   - to check whether the schema matches the data
   - to check whether the data has been correctly parsed and loaded
   - to check how NULL values, if any, are represented


2. Queries
   - `query_by_postcode(conn, postcode)`: return dataframe containing rows filtered from _pp_data_
   - `query_by_date(conn, start_date, end_date)`: return record containing the inner-joined data satisfying the specified date range  
   - `query_joined_year(conn, north, south, east, west, year)`: return record containing the inner-joined data satisfying the specified bbox and year
   - `query_joined_year_type(conn, north, south, east, west, year, property_type)`: return record containing the inner-joined data satisfying the specified bbox, year and property_type



3. `record_to_df(record)`/`short_record_to_df(record)`: add structure and tidy up the records returned from the queries above
   - change datatype of longitude/latitude for future use
   - remove unnecessary columns and add _log_price_
   - give informative labelling of the columns
   

4. `pca_df(data, n)`: returns dataframe obtained by standardising the features and fitting with _sklearn.decomposition.PCA(n)_
   - all columns of _data_ are considered to be features
   

## address.py
1. `choose_training_data(conn, latitude, longitude, year, property_type, box_size)`
   - use `query_joined_year` if _property_type_ is 'Other' and `query_joined_year_type` otherwise
   - increases _box_size_ by 0.05 up to 0.2 if the result of the query is smaller than 1000
   - raises an error if the result of the query is empty
     - the model cannot be extrapolated to predict price for point outside the range of dates/regions of UK Priced Paid data
   - returns _training_data_ and _box_size_
     - _box_size_ to be used later for defining bbox when calling `add_pois` 




2. `train_model(data)`
   - filter out 1% of the _data_ with the highest prices
   - _sm.OLS(data['log_price'], design)_



3. `predict_price(conn, latitude, longitude, year, property_type)`
   - call `choose_training_data` with initial _box_size=0.10_
     - prints out the size of the returned _training_data_
   - call `add_pois` with the _pred_point_ appended to the _training_data_
   - call `train_model` on data with _pred_point_ removed
   - returns _np.exp(results_basis.predict(design_pred))_
