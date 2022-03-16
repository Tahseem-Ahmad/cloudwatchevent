import pandas as pd
import json as json
import simplejson as sj
from datetime import datetime

excel_data_df = pd.read_excel('E:/TABLE_COLUMNS.xlsx', sheet_name='Sheet1', dtype=str)
# print(type(excel_data_df))

json_list = []


def dateToStr(param):
    if type(param) == str:
        yyyy, mm, dd = param.split(" ")[0].split("-")[0], param.split(" ")[0].split("-")[1], \
                       param.split(" ")[0].split("-")[2]
        return dd + '/' + mm + '/' + yyyy


for ind in excel_data_df.index:
    json_struct = {'RoleName': excel_data_df['RoleName'][ind],
                   'Active': excel_data_df['Active'][ind],
                   'InactiveDate': dateToStr(excel_data_df['InactiveDate'][ind]),
                   'LHCode': excel_data_df['LHEntityCode'][ind].split("-")[0],
                   'AuditRecInsertDate': dateToStr(excel_data_df['RecordInsertDate'][ind]),
                   'Env': excel_data_df['Env'][ind],
                   'Roleid': excel_data_df['RoleID'][ind],
                   'RoleInfo': {'authorization': {}, 'version': '0.0.1',
                                'lh_code': excel_data_df['LHEntityCode'][ind].split("-")[0]},
                   'AuditRecInsertBy': excel_data_df['RecordInsertedBy'][ind],
                   'AuditRecUpdby': excel_data_df['RecordUpdatedBy'][ind],
                   'LHEntityCode': excel_data_df['LHEntityCode'][ind],
                   'AuditRecUpDate': dateToStr(excel_data_df['RecordUpdateDate'][ind])}
    # print(ind)
    auth = excel_data_df['CoarsegrainedGroup'][ind]
    auth = {auth: {"fg_groups": [excel_data_df['FineGrainedGroup'][ind]],
                   "ndw_plan_code": [excel_data_df['NDWPlanCode'][ind]]}}
    json_struct['RoleInfo']['authorization'] = auth
    json_list.append(json_struct)

json_array = sj.dumps(json_list, ignore_nan=True)

print(json_array)
