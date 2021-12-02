import statsmodels.api as sm
from statsmodels.api import add_constant

from fynesse.assess import *
from fynesse.access import *


def choose_training_data(conn, latitude, longitude, year, property_type, box_size):
    start_date = str(year) + "-01-01"
    end_date = str(year+1) + "-01-01"
    if property_type == 'O':
        joined_data = join_pp_postcode(conn, latitude + box_size, latitude - box_size, longitude + box_size,
                                       longitude - box_size, start_date, end_date)
    else:
        joined_data = join_pp_postcode(conn, latitude + box_size, latitude - box_size, longitude + box_size,
                                       longitude - box_size, start_date, end_date, property_type)
    if len(joined_data) < 1000 and box_size < 0.2:
        joined_data, box_size = choose_training_data(conn, latitude, longitude, year, property_type, 0.2)
    if len(joined_data) == 0:
        raise ValueError("Unable to make a prediction")
    data = joined_record_to_df(joined_data)
    return data, box_size


def train_model(gp_data):
    data = gp_data[gp_data["price"] < gp_data["price"].quantile(0.99)]
    design = np.concatenate((data['ent'].values.reshape(-1, 1),
                             data['shop_amenity'].values.reshape(-1, 1),
                             data['healthcare'].values.reshape(-1, 1),
                             data['historic'].values.reshape(-1, 1),
                             data['public_transport'].values.reshape(-1, 1),
                             data['tourism'].values.reshape(-1, 1)), axis=1)
    design = add_constant(design)
    m_linear_basis = sm.OLS(data['log_price'], design)
    results_basis = m_linear_basis.fit()
    return results_basis


def predict_price(conn, latitude, longitude, year, property_type):
    pred_point = {'latitude': latitude, 'longitude': longitude}
    training_data, box_size = choose_training_data(conn, latitude, longitude, year, property_type, 0.15)
    data = training_data.append(pred_point, ignore_index=True)
    tag = [{'leisure': True}, {'sport': True}, {'healthcare': True}, {'historic': True}, {'public_transport': True},
           {'tourism': True}, {'shop': True, 'amenity': True}]
    name = ['leisure', 'sport', 'healthcare', 'historic', 'public_transport', 'tourism', 'shop_amenity']
    data = add_pois(data, tag, name, 0.05, latitude + box_size, latitude - box_size, longitude + box_size,
                    longitude - box_size)
    data['ent'] = data['leisure'] + data['sport']
    data = data.fillna(0)
    gp_data = data[:-1]
    results_basis = train_model(gp_data)
    gp_point = gp_data.iloc[-1]
    design_pred = np.array([gp_point['ent'], gp_point['shop_amenity'], gp_point['healthcare'], gp_point['historic'],
                            gp_point['public_transport'], gp_point['tourism']])
    design_pred = add_constant(design_pred)
    pred = results_basis.predict(design_pred)
    return np.exp(pred)
