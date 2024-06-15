import pandas as pd
from config import *

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


def csv_find_trades_start_end_rows():
    with open(CSV_REPORT_PATH, "r", encoding='utf-8') as f:
        lines = f.readlines()

    first_df_row_number = None
    last_df_row_number = len(lines) - 1

    for i, line in enumerate(lines):
        # find first row = header of the dataframe
        if not first_df_row_number and OPTION_DATA_INDICATOR_STRING in line:
            first_df_row_number = i - 1

        # find last row = first row which did not contain the OPTION_DATA_INDICATOR_STRING after the first occurance found
        if first_df_row_number and OPTION_DATA_INDICATOR_STRING not in line:
            last_df_row_number = i - 1

    if None in (first_df_row_number, last_df_row_number):
        raise Exception("Invalid .csv file no first or last row found")

    return first_df_row_number, last_df_row_number


def print_trades_overview(df: pd.DataFrame):
    erlös_eur_total = 0
    prov_eur_total = 0
    buy_to_open_erlös_eur_total = df.loc[df['buy_to_open'], 'Erlös / EUR'].sum()
    buy_to_open_prov_eur_total = df.loc[df['buy_to_open'], 'Prov./Gebühr / EUR'].sum()
    sell_to_close_erlös_eur_total = df.loc[df['sell_to_close'], 'Erlös / EUR'].sum()
    sell_to_close_prov_eur_total = df.loc[df['sell_to_close'], 'Prov./Gebühr / EUR'].sum()

    distinct_symbols = df["Symbol"].unique().tolist()

    for i, symbol in enumerate(distinct_symbols):
        # get date sorted sub dataframe which contains one symbol
        sub_df = df[df["Symbol"] == symbol].copy()
        sub_df.sort_values(by=['Datum/Zeit'], inplace=True)

        print("#" * 153)
        print(f"{i} Symbol")
        print(f"{symbol}")
        print(sub_df)

        erlös_eur_total += sub_df["Erlös / EUR"].sum()
        prov_eur_total += sub_df["Prov./Gebühr / EUR"].sum()

    print(f"erlös_eur_total: {erlös_eur_total}")
    print(f"prov_eur_total: {prov_eur_total}")
    print(f"buy_to_open_erlös_eur_total: {buy_to_open_erlös_eur_total}")
    print(f"buy_to_open_prov_eur_total: {buy_to_open_prov_eur_total}")
    print(f"sell_to_close_erlös_eur_total: {sell_to_close_erlös_eur_total}")
    print(f"sell_to_close_prov_eur_total: {sell_to_close_prov_eur_total}")


def load_and_clean_trades_df():
    # determine the start and end of the relevant data
    first_df_row_number, last_df_row_number = csv_find_trades_start_end_rows()

    # csv -> df
    num_rows = last_df_row_number - first_df_row_number
    df = pd.read_csv(CSV_REPORT_PATH, skiprows=first_df_row_number, nrows=num_rows)

    # filter df only for Trade rows
    df = df[df["DataDiscriminator"] == 'Trade']

    # just keep relevant columns - relevant for readability and further calculations
    relevant_columns = [
        'Symbol',
        'Datum/Zeit',
        'Menge',
        'Erlös',
        'Prov./Gebühr',
        'Code'
    ]

    df = df[relevant_columns]

    # change date format to yyyymmdd
    df['Datum/Zeit'] = pd.to_datetime(df['Datum/Zeit'], format='%Y-%m-%d, %H:%M:%S').dt.strftime('%Y%m%d')


    return df


def load_and_clean_usd_eur_df():
    df_usd_eur = pd.read_csv(CSV_USD_EUR_PATH)

    # 'AccountAlias' column because there are multiple tables in the .csv and this is the header for the later
    # relevant table
    df_usd_eur = df_usd_eur[df_usd_eur['AccountAlias'] == 'USD']

    # keep and rename relevant columns and drop the others
    df_usd_eur = df_usd_eur.iloc[:, :4]
    df_usd_eur.columns = ["Date", "FromCurrency", "ToCurrency", "Rate"]

    return df_usd_eur


def is_buy_to_open(row):
    return row['Menge'] > 0 and 'O' in row['Code']


def is_sell_to_close(row):
    return row['Menge'] < 0 and 'C' in row['Code']


def merge_dfs(df: pd.DataFrame, df_usd_eur: pd.DataFrame):
    # merge
    df = pd.merge(df, df_usd_eur, left_on='Datum/Zeit', right_on='Date', how='left')

    # drop not needed columns from the usd_eur df
    df.drop(["Date", "FromCurrency", "ToCurrency"], axis=1, inplace=True)

    # rename some columns with USD suffix
    df.rename(inplace=True, columns={
        'Rate': 'usd_eur_rate',
        "Erlös": "Erlös / USD",
        "Prov./Gebühr": "Prov./Gebühr / USD",
    })

    # cast exchange rate to float
    df["usd_eur_rate"] = df["usd_eur_rate"].astype(float)

    # add EUR columns
    df["Erlös / EUR"] = df["Erlös / USD"] * df["usd_eur_rate"]
    df["Prov./Gebühr / EUR"] = df["Prov./Gebühr / USD"] * df["usd_eur_rate"]

    # make buy to open and sell to close columns
    df['buy_to_open'] = df.apply(is_buy_to_open, axis=1)
    df['sell_to_close'] = df.apply(is_sell_to_close, axis=1)

    # sort by date
    # df.sort_values(by=['Datum/Zeit'], inplace=True)
    return df

def store_df_excel(df: pd.DataFrame):
    df.to_excel(EXCEL_OUTPUT_PATH, index=False)