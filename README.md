# Fynesse ADS


## Access

1. UK Price Paid data : 1995-2021 uploaded to table `pp_data` of the specified databse

2. Postcode

3. Joined Table

4. OpenStreetMap : add count of different pois to the given dataframe/point of latitude + longitude

### Test
- whether the parsing has been correctly done

## Assess
1. Print sample rows by specifying the tables

2. Query db by year

3. Query by postcode (not joined)

4. 

### Test

- datatype of each column to do the operation on

- how to deal with null values


## Address
1. Training data + fit the data to the linear model : query differs slightly based on the inputs

2. prediction


### Test

- size of the training data

- cannot make prediction outside the range - training data is defined to be of the same year 

- the bounds of the accepted inputs : raises an error if the size of the query is 0, warning if lower than 1000