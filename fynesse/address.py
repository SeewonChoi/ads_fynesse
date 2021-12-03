import statsmodels.api as sm
from statsmodels.api import add_constant

from fynesse.assess import *
from fynesse.access import *


def choose_training_data(conn, latitude, longitude, year, property_type, box_size):
    if property_type == 'O':
        joined_data = query_join_by_year(conn, latitude + box_size, latitude - box_size, longitude + box_size,
                                         longitude - box_size, year)
    else:
        joined_data = query_join_by_year_type(conn, latitude + box_size, latitude - box_size, longitude + box_size,
                                              longitude - box_size, year, property_type)
    if len(joined_data) < 1000 and box_size < 0.2:
        joined_data, box_size = choose_training_data(conn, latitude, longitude, year, property_type, box_size + 0.05)
    if len(joined_data) == 0:
        raise ValueError("No matching training data. Unable to make a prediction")
    data = short_record_to_df(joined_data)
    return data, box_size


def train_model(gp_data):
    data = gp_data[gp_data["price"] < gp_data["price"].quantile(0.99)]
    design = np.column_stack((data['ent'], data['shop_amenity'],
                              data['healthcare'],
                              data['historic'],
                              data['public_transport'],
                              data['tourism']))
    design = add_constant(design)
    m_linear_basis = sm.OLS(data['log_price'], design)
    results_basis = m_linear_basis.fit()
    return results_basis


def predict_price(conn, latitude, longitude, year, property_type):
    pred_point = {'latitude': latitude, 'longitude': longitude}
    training_data, box_size = choose_training_data(conn, latitude, longitude, year, property_type, 0.15)
    data = training_data.append(pred_point, ignore_index=True)
    print("The size of the training data was " + str(len(data)))

    tag = [{'leisure': True}, {'sport': True}, {'healthcare': True}, {'historic': True}, {'public_transport': True},
           {'tourism': True}, {'shop': True, 'amenity': True}]
    name = ['leisure', 'sport', 'healthcare', 'historic', 'public_transport', 'tourism', 'shop_amenity']
    data = add_pois(data, tag, name, 0.005, latitude + box_size, latitude - box_size,
                    longitude + box_size, longitude - box_size)
    data['ent'] = data['leisure'] + data['sport']
    data = data.fillna(0)

    gp_data = data[:-1]
    results_basis = train_model(gp_data)

    gp_point = data.iloc[-1]
    design_pred = [1, gp_point['ent'], gp_point['shop_amenity'], gp_point['healthcare'], gp_point['historic'],
                   gp_point['public_transport'], gp_point['tourism']]
    pred = results_basis.predict(design_pred)
    return np.exp(pred)[0]