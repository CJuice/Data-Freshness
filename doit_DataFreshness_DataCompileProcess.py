"""

"""


def main():
    import os
    import pandas as pd
    import datetime
    import json

    agol_dataframe_excel = None
    agol_dataframe_json = None
    agol_excel_file_name = "AGOL_data_freshness.xlsx"
    agol_json_file_name = "AGOL_data_freshness.json"
    compiled_json_file_name = "DataCompiled.json"
    data_file_dir = r"Docs\DataFreshnessOutputs"
    socrata_dataframe_excel = None
    socrata_dataframe_json = None
    socrata_excel_file_name = "SOCRATA_data_freshness.xlsx"
    socrata_json_file_name = "SOCRATA_data_freshness.json"

    combined_data_file_name_excel = f"{data_file_dir}\dataFreshness_{datetime.datetime.now().strftime('%Y_%m_%d')}.xlsx"
    combined_data_file_name_json = f"{data_file_dir}\{compiled_json_file_name}"

    # Excel Portion
    for dirname, dirs, files in os.walk(data_file_dir):
        for file in files:
            if file == agol_excel_file_name:
                agol_dataframe_excel = pd.read_excel(os.path.join(dirname, file))
            elif file == socrata_excel_file_name:
                socrata_dataframe_excel = pd.read_excel(os.path.join(dirname, file))

    master_data_freshness_df_excel = pd.concat([agol_dataframe_excel, socrata_dataframe_excel])
    print(master_data_freshness_df_excel.info())
    master_data_freshness_df_excel.to_excel(excel_writer=combined_data_file_name_excel,
                                            index=False)

    # JSON Portion
    for dirname, dirs, files in os.walk(data_file_dir):
        for file in files:
            if file == agol_json_file_name:
                with open(os.path.join(dirname, file), 'r') as agol_handler:
                    agol_json = json.loads(agol_handler.read())
                # agol_dataframe_json = pd.read_json(path_or_buf=os.path.join(dirname, file),
                #                                    orient="records")
            elif file == socrata_json_file_name:
                with open(os.path.join(dirname, file), 'r') as socrata_handler:
                    socrata_json = json.loads(socrata_handler.read())
                # socrata_dataframe_json = pd.read_json(os.path.join(dirname, file), orient="records")
        break
    combined_json_contents = agol_json + socrata_json
    with open(combined_data_file_name_json, 'w') as compiled_handler:
        compiled_handler.write(json.dumps(combined_json_contents))


if __name__ == "__main__":
    main()
