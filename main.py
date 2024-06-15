from util import *

if __name__ == "__main__":
    # load and clean trade and usd_eur_exchangerate dataframes
    df = load_and_clean_trades_df()
    df_usd_eur = load_and_clean_usd_eur_df()

    # merge dataframes
    df = merge_dfs(df, df_usd_eur)

    # print and store results
    print_trades_overview(df)
    store_df_excel(df)



