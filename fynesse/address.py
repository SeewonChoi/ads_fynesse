import statsmodels.api as sm
import numpy as np

from fynesse.assess import *
from fynesse.access import *


def predict_price(conn, latitude, longitude, year, property_type):
    results_basis = train_data(latitude, longitude, year, property_type, 0.15, conn)
    # gp_point = add_pois_point(latitude, longitude)
    # design_pred = np.array([gp_point])
    # return results_basis.predict(design_pred)
    return 1


def train_data(latitude, longitude, year, property_type, box_size, conn):
    if property_type == 'O':
        joined_data = join_pp_postcode_other(conn, longitude - box_size, longitude + box_size, latitude - box_size,
                                             latitude + box_size, year)
    else:
        joined_data = join_pp_postcode(conn, longitude - box_size, longitude + box_size, latitude - box_size,
                                       latitude + box_size, year, property_type)
    if len(joined_data) < 1000:
        if box_size == 0.15:
            train_data(latitude, longitude, year, property_type, 0.2)
        else:
            raise "Unable to make a prediction."
    else:
        joined_data = tidy_joined_df(joined_data)
        t = ["healthcare", "historic", "leisure", "public_transport", "railway", "sport"]
        gp_data = add_pois(joined_data, t, box_size, latitude, longitude)
        gp_data = gp_data.fillna(0)
        data = gp_data[gp_data["price"] < gp_data["price"].quantile(0.99)]

        design = np.concatenate(
            (data['count_facil'].values.reshape(-1, 1), data['count_historic'].values.reshape(-1, 1),
             data['count_public_transport'].values.reshape(-1, 1),
             data['count_railway'].values.reshape(-1, 1)), axis=1)
        m_linear_basis = sm.OLS(data['price'], design)
        results_basis = m_linear_basis.fit()
    return results_basis


"""
d = {'latitude': [latitude], 'longitude': [longitude], 'date' : [year], 'property_type': [property_type]}
df_point = pd.DataFrame(data=d)

geometry=gpd.points_from_xy(df_point.longitude, df_point.latitude)
gp_point = gpd.GeoDataFrame(df_point, geometry=geometry)
gp_point.crs = "EPSG:4326"

gp_point['geometry'] = gp_point['geometry'].buffer(0.005)

t = ["healthcare", "historic", "leisure", "public_transport", "railway", "sport"]
for tag in t:
  tags = {tag:True}
  col_name = "count_"+tag
  pois = ox.geometries_from_bbox(latitude+0.2, latitude-0.2, longitude+0.2, longitude-0.2, tags)
  pois.reset_index(inplace=True)
  pois.set_index('osmid', inplace=True)
  gp_data[col_name] = sjoin(gp_data, pois, how='left').groupby(['index']).count()['price']
  gp_point[col_name] = len(sjoin(gp_data, pois, how='left'))
  
gp_data['count_facil'] = gp_data['count_leisure'] + gp_data['count_healthcare'] + gp_data['count_sport']
gp_point['count_facil'] = gp_point['count_leisure'] + gp_point['count_healthcare'] + gp_point['count_sport']
data = gp_data

design = np.concatenate((data['count_facil'].values.reshape(-1,1), data['count_historic'].values.reshape(-1,1), data['count_public_transport'].values.reshape(-1,1), data['count_railway'].values.reshape(-1,1)),axis=1)
m_linear_basis = sm.OLS(data['price'],design)
results_basis = m_linear_basis.fit()
design_pred = np.array([gp_point.iat[0,0], gp_point.iat[0,0], gp_point.iat[0,0], gp_point.iat[0,0]])
print(results_basis.predict(design_pred))

"""