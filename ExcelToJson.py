import pandas as pd
import json as json

excel_data_df = pd.read_excel('E:/TABLE_COLUMNS.xlsx', sheet_name='Sheet1')
print(type(excel_data_df))
json_struct = {
    "RoleName": "",
    "Active": 0,
    "InactiveDate": "",
    "LHCode": "LH001",
    "AuditRecInsertDate": "2/07/2022",
    "Env": "Lab",
    "Roleid": 0,
    "RoleInfo": {
        "authorization": {
            "bcbsa_bcp_edar_cg": {
                "fg_groups": [
                    "edar_std"
                ],
                "ndw_plan_code": [
                    "425"
                ]
            }
        },
        "version": "0.0.1",
        "lh_code": "LH001"
    },
    "AuditRecInsertBy": "",
    "AuditRecUpdby": "",
    "LHEntityCode": "",
    "AuditRecUpDate": ""
}
json_array = list()

for ind in excel_data_df.index:
    json_struct['RoleName'] = excel_data_df['RoleName'][ind]
    json_struct['Active'] = excel_data_df['Active'][ind]
    json_struct['InactiveDate'] = pd.to_datetime(excel_data_df['InactiveDate'][ind], unit='ms')
    json_struct['LHCode'] = excel_data_df['LHEntityCode'][ind].split("-")[0]
    json_struct['AuditRecInsertDate'] = pd.to_datetime(excel_data_df['RecordInsertDate'][ind], unit='ms')
    json_struct['Env'] = excel_data_df['Env'][ind]
    json_struct['Roleid'] = excel_data_df['RoleID'][ind]
    json_struct['AuditRecInsertBy'] = excel_data_df['RecordInsertedBy'][ind]
    json_struct['AuditRecUpdby'] = excel_data_df['RecordUpdatedBy'][ind]
    json_struct['LHEntityCode'] = excel_data_df['LHEntityCode'][ind]
    json_struct['AuditRecUpDate'] = pd.to_datetime(excel_data_df['RecordUpdateDate'], unit='ms')
    json_array.append(json_struct)

# json_str = excel_data_df.to_json(orient='records')
print(json_array)
json_struct['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['ndw_plan_code'] = ["430"]
print(json_struct['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['ndw_plan_code'])
