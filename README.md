# Fynesse ADS


## Access

1. Functions to create the tables
- `create_table_pp(conn)`
- `create_table_postcode(conn)`
- `create_table_prices_coord(conn)`

2. Function to delete tables
- `drop_table(conn)`

3. Functions to upload data into the tables
- `data_pp(conn)` : UK Priced Paid data from 1995 to 2021
- `data_postcode(conn)` : postcode
- `join_pp_postcode(conn, north, south, east, west, start_date, end_date)` + `data_joined(record, conn)` :  inner join on postcode

4. Functions to create indexes on tables
- `create_index_pp(conn)`
- `create_index_postcode(conn`

5. Functions to augment dataframe with OpenStreetMap data
- `to_gdf(data, dist)`
- `add_pois(df, tag_list, name_list, dist, north, south, east, west)`
- adds count for each tag in the input tag_list

### To be tested
- whether the parsing has been correctly done

## Assess
1. Tidy up the results of database table look up : returned in record, functions to change it to dataframe with sensible column names
- `joined_record_to_df(record)`
- only selection of the columns returned : price, date, postcode, type, latitude, longitude

2. Print sample rows by specifying the tables : to check whether the data has been correctly uploaded/parsed
- `head(conn, table, n)`

3. Different queries to the tables
- `query_by_date(conn, start_date, end_date)` : join without 
- `query_by_postcode(conn, postcode)`
- `query_joined_year(conn, north, south, east, west, year)`
- `query_joined_year_type(conn, north, south, east, west, year, property_type)`


4. Principal Component Analysis
- `pca_df(data, n, filter_prices=False)`

### To be tested

- datatype of each column to do the operation on

- how to deal with null values


## Address
1. `choose_training_data`

2. `train_model(gp_data)`

3. `predict_price(conn, latitude, longitude, year, property_type)`


### To be tested
- size of the training data

- cannot make prediction outside the range - training data is defined to be of the same year 

- the bounds of the accepted inputs : raises an error if the size of the query is 0, warning if lower than 1000