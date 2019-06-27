"""

"""


def main():
    import os
    import pandas as pd
    import datetime
    import json

    agol_dataframe_excel = None
    agol_excel_file_name = "AGOL_data_freshness.xlsx"
    agol_json_file_name = "AGOL_data_freshness.json"
    compiled_json_file_name = "DataCompiled.json"
    data_file_dir = r"Docs\DataFreshnessOutputs"
    null_string = "NULL"
    socrata_dataframe_excel = None
    socrata_excel_file_name = "SOCRATA_data_freshness.xlsx"
    socrata_json_file_name = "SOCRATA_data_freshness.json"

    combined_data_file_name_excel = f"{data_file_dir}\dataFreshness_{datetime.datetime.now().strftime('%Y_%m_%d')}.xlsx"
    combined_data_file_name_json = f"{data_file_dir}\{compiled_json_file_name}"

    # Excel Portion
    for dirname, dirs, files in os.walk(data_file_dir):
        for file in files:
            if file == agol_excel_file_name:
                agol_dataframe_excel = pd.read_excel(io=os.path.join(dirname, file), na_values=null_string)
            elif file == socrata_excel_file_name:
                socrata_dataframe_excel = pd.read_excel(io=os.path.join(dirname, file), na_values=null_string)

    master_data_freshness_df_excel = pd.concat([agol_dataframe_excel, socrata_dataframe_excel])

    # Unsure why NULL values are dropped on read but have to fill before exporting
    master_data_freshness_df_excel.fillna(value=null_string, inplace=True)
    print(master_data_freshness_df_excel.info())
    master_data_freshness_df_excel.to_excel(excel_writer=combined_data_file_name_excel,
                                            index=False)

    # JSON Portion
    for dirname, dirs, files in os.walk(data_file_dir):
        for file in files:
            if file == agol_json_file_name:
                with open(os.path.join(dirname, file), 'r') as agol_handler:
                    agol_json = json.loads(agol_handler.read())
            elif file == socrata_json_file_name:
                with open(os.path.join(dirname, file), 'r') as socrata_handler:
                    socrata_json = json.loads(socrata_handler.read())
        break # Just making sure
    combined_json_contents = agol_json + socrata_json
    with open(combined_data_file_name_json, 'w') as compiled_handler:
        compiled_handler.write(json.dumps(combined_json_contents))


if __name__ == "__main__":
    main()
