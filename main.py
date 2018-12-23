def get_ynab_month(month):
    import ynab

    configuration = ynab.Configuration()
    configuration.api_key['Authorization'] = get_config_val('ynab_api_key')
    configuration.api_key_prefix['Authorization'] = 'Bearer'
    api_instance = ynab.MonthsApi(ynab.ApiClient(configuration))
    budget_id = get_config_val('ynab_budget_id')
    api_response = api_instance.get_budget_month(budget_id, month)
    return api_response

def get_config_val(key):
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['main'][key]

def extract_ynab_cat_attrs(month, api_response):
    cat_attrs = []
    for category in api_response.data.month.categories:
        name = category.name
        budgeted = category.budgeted / 1000.0
        cat_attrs.append((month, name, budgeted))
    return cat_attrs

def test_ynab():
    import pprint
    month = '2018-12-01'
    api_response = get_ynab_month(month)
    cat_attrs = extract_ynab_cat_attrs(month, api_response)
    pprint.pprint(cat_attrs)

def main():

main()