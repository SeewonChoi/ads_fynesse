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
    if len(joined_data) == 0:
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
