import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

REPORT_PATH = "data/report.csv"
OPTION_DATA_INDICATOR_STRING = "Aktien- und Indexoptionen"

def csv_find_df_start_end_rows():
    with open(REPORT_PATH, "r", encoding='utf-8') as f:
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

def print_symbol_rows():
    erlös = df["Erlös"].sum()
    prov = df["Prov./Gebühr"].sum()
    basis = df["Basis"].sum()
    real_guv = df["Realisierter G&V"].sum()
    mtmguv = df["MTM-G&V"].sum()

    for i, symbol in enumerate(distinct_symbols):
        sub_df = df[df["Symbol"] == symbol]

        print("#"*80)
        print(f"{i} Symbol")
        print(f"{symbol}: {symbol}")
        print(sub_df)

    print("#"*80)
    print(f"Total Erlös: {erlös}")
    print(f"Total Prov./Gebühr: {prov}")
    print(f"Total Basis: {basis}")
    print(f"Total Realisierter G&V: {real_guv}")
    print(f"Total MTM-G&V: {mtmguv}")

# determine the start and end of the relevant data
first_df_row_number, last_df_row_number = csv_find_df_start_end_rows()

# csv -> df
num_rows = last_df_row_number - first_df_row_number
df = pd.read_csv(REPORT_PATH, skiprows=first_df_row_number, nrows=num_rows)

# filter df only for Trade rows
#df = df[df['Code'].str.contains('C|O', na=False)]
df = df[df["DataDiscriminator"] == 'Trade']

distinct_symbols = df["Symbol"].unique().tolist()

print_symbol_rows()
pass


