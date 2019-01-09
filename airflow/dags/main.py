from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.utils import dates

def get_ynab_month(month):
    """
    Gets YNAB data

    Retrieves YNAB budget API data pertaining to given month.

    Args:
        month: Month we want data for, as YYYY-MM-DD string

    Returns:
        A YNAB API MonthDetailResponse object, which includes budget and
        category-level data.
    """

    import ynab

    configuration = ynab.Configuration()
    configuration.api_key['Authorization'] = get_config_val('ynab_api_key')
    configuration.api_key_prefix['Authorization'] = 'Bearer'
    api_instance = ynab.MonthsApi(ynab.ApiClient(configuration))
    budget_id = get_config_val('ynab_budget_id')
    api_response = api_instance.get_budget_month(budget_id, month)
    return api_response

def get_config_val(key):
    """
    Gets a config value, given a key

    Looks up value from config file given a key. Used for secrets like API keys.

    Args:
        key: key for config value

    Returns:
        value: corresponding value stored in config file for given key
    """

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

def colnum_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def copy_bucket_lookup_formula(month, wks):
    import gspread
    formula_col_num = 4
    formula_col = colnum_string(formula_col_num)
    formula_row = 2
    formula_cell = '{}{}'.format(formula_col, formula_row)
    formula = wks.acell(formula_cell, value_render_option='FORMULA').value

    lookup_col = 'B'
    lookup_cell = '{}{}'.format(lookup_col, formula_row)
    
    new_month_cells = wks.findall(month)
    new_formula_cells = []
    
    for month_cell in new_month_cells:
        row = month_cell.row
        relative_lookup_cell = '{}{}'.format(lookup_col, row)
        relative_formula_cell = '{}{}'.format(formula_col, row)
        relative_formula = formula.replace(lookup_cell, relative_lookup_cell)
        new_formula_cells.append(gspread.models.Cell(row, formula_col_num, relative_formula))
    wks.update_cells(new_formula_cells, value_input_option='USER_ENTERED')

def print_the_date(ds, **kwargs):
    import logging
    from pprint import pprint

    logging.info(ds)


def ynab(ds, **kwargs):
    month = '2018-12-01'
    sheet_name = 'Data'

    ynab_api_response = get_ynab_month(month)
    ynab_data = extract_ynab_cat_attrs(month, ynab_api_response)
    sh = get_gspread_wks()
    wks = sh.worksheet(sheet_name)
    del_existing_month(month, wks)
    insert_new_data(ynab_data, sh, sheet_name)
    copy_bucket_lookup_formula(month, wks)

default_args = {
    'owner': 'kyle',
    'depends_on_past': False,
    'start_date': dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='ynab',
          default_args=default_args,
          schedule_interval='*/5 * * * *',
          dagrun_timeout=timedelta(minutes=1))

ynab_task = PythonOperator(
    task_id='ynab_task',
    provide_context=True,
    python_callable=print_the_date,
    dag=dag)

