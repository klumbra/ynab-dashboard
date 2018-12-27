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
        cat_attrs.append([month, name, budgeted])
    return cat_attrs

def test_ynab():
    import pprint
    month = '2018-12-01'
    api_response = get_ynab_month(month)
    cat_attrs = extract_ynab_cat_attrs(month, api_response)
    pprint.pprint(cat_attrs)

def get_gspread_wks():
    from oauth2client.service_account import ServiceAccountCredentials
    import gspread

    scopes = ['https://www.googleapis.com/auth/spreadsheets']

    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scopes)
    gc = gspread.authorize(creds)

    sheet_id = get_config_val('sheet_id')
    return gc.open_by_key(sheet_id)

def del_existing_month(month, wks):
    import pprint
    cell_list = wks.findall(month)
    
    for cell in reversed(cell_list):
        wks.delete_row(cell.row) # slow given separate API call per del

def next_available_row(wks):
    str_list = list(filter(None, wks.col_values(1)))
    return str(len(str_list)+1)

def insert_new_data(ynab_data, sh, sheet_name):
    import gspread
    next_row = next_available_row(sh.worksheet(sheet_name))
    sh.values_update(
        '{}!A{}'.format(sheet_name, next_row),
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': ynab_data}
    )

def copy_bucket_lookup_formula(month, wks):
    formula_col = 'D'
    formula_row = 2
    formula_cell = '{}{}'.format(formula_col, formula_row)
    formula = wks.acell(formula_cell, value_render_option='FORMULA').value

    lookup_col = 'B'
    lookup_cell = '{}{}'.format(lookup_col, formula_row)
    
    new_month_cells = wks.findall(month)
    
    for cell in new_month_cells:
        row = cell.row
        relative_lookup_cell = '{}{}'.format(lookup_col, row)
        relative_formula_cell = '{}{}'.format(formula_col, row)
        relative_formula = formula.replace(lookup_cell, relative_lookup_cell)
        wks.update_acell(relative_formula_cell, relative_formula)

month = '2018-12-01'
sheet_name = 'Data'

ynab_api_response = get_ynab_month(month)
ynab_data = extract_ynab_cat_attrs(month, ynab_api_response)
sh = get_gspread_wks()
wks = sh.worksheet(sheet_name)
del_existing_month(month, wks)
insert_new_data(ynab_data, sh, sheet_name)
copy_bucket_lookup_formula(month, wks)
