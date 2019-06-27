"""

"""


def main():
    import os
    import pandas as pd
    import datetime

    agol_file_name = "AGOL_data_freshness.xlsx"
    socrata_file_name = "SOCRATA_data_freshness.xlsx"
    data_file_dir = r"Docs\DataFreshnessOutputs"
    combined_data_file_name = f"{data_file_dir}\dataFreshness{datetime.datetime.now().strftime('%Y_%m_%d')}.xlsx"

    agol_dataframe = None
    socrata_dataframe = None
    for dirname, dirs, files in os.walk(data_file_dir):
        for file in files:
            if file == agol_file_name:
                agol_dataframe = pd.read_excel(os.path.join(dirname, file))
            elif file == socrata_file_name:
                socrata_dataframe = pd.read_excel(os.path.join(dirname, file))

    master_data_freshness_df = pd.concat([agol_dataframe, socrata_dataframe])
    print(master_data_freshness_df.info())
    master_data_freshness_df.to_excel(excel_writer=combined_data_file_name,
                                      index=False)


if __name__ == "__main__":
    main()
