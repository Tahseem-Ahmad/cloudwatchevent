import json
from jsonschema import validate
import numpy as np
import pandas as pd
import boto3 as boto3
import simplejson as sj
from datetime import datetime

from numpy import nan

excel_data_df = pd.read_excel('E:/TABLE_COLUMNS.xlsx', sheet_name='Sheet1', dtype=str)
print(str(len(excel_data_df)))
# print(excel_data_df)
json_list = []

#
# dynamoDb = boto3.resource('dynamoDB')
# table = dynamoDb.Table('USERS')
# # table.put_item()
# table.batch_writer()

json_dict = {}


def dateToStr(param):
    if type(param) == str:
        yyyy, mm, dd = param.split(" ")[0].split("-")[0], param.split(" ")[0].split("-")[1], \
                       param.split(" ")[0].split("-")[2]
        return dd + '/' + mm + '/' + yyyy


for ind in excel_data_df.index:
    if excel_data_df['RoleName'][ind] in json_dict:
        existing_json = json_dict[excel_data_df['RoleName'][ind]]
        auth = excel_data_df['CoarsegrainedGroup'][ind]
        auth = auth.strip()
        if auth == 'bcbsa_bcp_edar_cg':
            if auth in existing_json['RoleInfo']['authorization']:
                fg_group = existing_json['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['fg_groups']
                fg_group.append(excel_data_df['FineGrainedGroup'][ind])
                existing_json['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['fg_groups'] = fg_group
                ndw_plan_code = existing_json['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['ndw_plan_code']
                ndw_plan_code.append(excel_data_df['NDWPlanCode'][ind])
                existing_json['RoleInfo']['authorization']['bcbsa_bcp_edar_cg']['ndw_plan_code'] = ndw_plan_code
            else:
                temp_auth = existing_json['RoleInfo']['authorization']
                temp_auth[auth] = {"fg_groups": [excel_data_df['FineGrainedGroup'][ind]],
                                   "ndw_plan_code": [excel_data_df['NDWPlanCode'][ind]]}
                existing_json['RoleInfo']['authorization'] = temp_auth
        elif auth == 'bcbsa_bcp_rtm_cg':
            if auth in existing_json['RoleInfo']['authorization']:
                fg_group = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['fg_groups']
                fg_group.append(excel_data_df['FineGrainedGroup'][ind])
                existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['fg_groups'] = fg_group
            else:
                temp_auth = existing_json['RoleInfo']['authorization']
                temp_auth[auth] = {"fg_groups": [excel_data_df['FineGrainedGroup'][ind]]}
                existing_json['RoleInfo']['authorization'] = temp_auth

            if pd.notnull(excel_data_df['ControlPlanCode'][ind]) | pd.notnull(excel_data_df['ControlStationCode'][ind]):
                plan_code = {'plan_code': excel_data_df['ControlPlanCode'][ind],
                             'station_code': excel_data_df['ControlStationCode'][ind]}
                control = {'codes': {'control': [plan_code]}}
                if 'claim' in existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']:
                    claim_list = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim']
                    last_claim = claim_list[len(claim_list) - 1]
                    last_claim['codes']['control'] = [plan_code]
                    existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim'][len(claim_list) - 1] = last_claim
                else:
                    temp_auth = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']
                    temp_auth['claim'] = [control]
                    existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg'] = temp_auth
            if pd.notnull(excel_data_df['ProcessingPlanCode'][ind]) | pd.notnull(
                    excel_data_df['ProcessingStationCode'][ind]):
                plan_code = {'plan_code': excel_data_df['ProcessingPlanCode'][ind],
                             'station_code': excel_data_df['ProcessingStationCode'][ind]}
                processing = {'codes': {'processing': [plan_code]}}
                if 'claim' in existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']:
                    claim_list = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim']
                    last_claim = claim_list[len(claim_list) - 1]
                    last_claim['codes']['processing'] = [plan_code]
                    existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim'][len(claim_list) - 1] = last_claim
                    #print(existing_json)
                else:
                    temp_auth = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']
                    temp_auth['claim'] = [processing]
                    existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg'] = temp_auth
            if 'claim' in existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']:
                claim_list = existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim']
                last_claim = claim_list[len(claim_list) - 1]
                last_claim['privacy'] = {}
                last_claim['boid'] = excel_data_df['BOID'][ind]
                existing_json['RoleInfo']['authorization']['bcbsa_bcp_rtm_cg']['claim'][
                    len(claim_list) - 1] = last_claim
        #print(existing_json)
    else:
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
        auth = excel_data_df['CoarsegrainedGroup'][ind]
        auth = auth.strip()
        if auth == 'bcbsa_bcp_edar_cg':
            auth = {auth: {"fg_groups": [excel_data_df['FineGrainedGroup'][ind]],
                           "ndw_plan_code": [excel_data_df['NDWPlanCode'][ind]]}}
        elif auth == 'bcbsa_bcp_rtm_cg':
            auth = {auth: {"fg_groups": [excel_data_df['FineGrainedGroup'][ind]]}}
            if pd.notnull(excel_data_df['ControlPlanCode'][ind]) | pd.notnull(excel_data_df['ControlStationCode'][ind]):
                plan_code = {'plan_code': excel_data_df['ControlPlanCode'][ind],
                             'station_code': excel_data_df['ControlStationCode'][ind]}
                codes = {'codes': {'control': [plan_code]}}
                temp_auth = auth['bcbsa_bcp_rtm_cg']
                temp_auth['claim'] = [codes]
                auth['bcbsa_bcp_rtm_cg'] = temp_auth
            if pd.notnull(excel_data_df['ProcessingPlanCode'][ind]) | pd.notnull(
                    excel_data_df['ProcessingStationCode'][ind]):
                plan_code = {'plan_code': excel_data_df['ProcessingPlanCode'][ind],
                             'station_code': excel_data_df['ProcessingStationCode'][ind]}
                codes = {'codes': {'processing': [plan_code]}}
                # print(codes)
                if 'claim' in auth['bcbsa_bcp_rtm_cg']:
                    temp_code = auth['bcbsa_bcp_rtm_cg']['claim'][0]['codes']
                    temp_code['processing'] = [plan_code]
                    auth['bcbsa_bcp_rtm_cg']['claim'][0]['codes'] = temp_code
                else:
                    temp_auth = auth['bcbsa_bcp_rtm_cg']
                    temp_auth['claim'] = [codes]
                    auth['bcbsa_bcp_rtm_cg'] = temp_auth
            #print(auth)
            if 'claim' in auth['bcbsa_bcp_rtm_cg']:
                temp_auth = auth['bcbsa_bcp_rtm_cg']['claim'][0]
                temp_auth['privacy'] = {}
                temp_auth['boid'] = excel_data_df['BOID'][ind]
                auth['bcbsa_bcp_rtm_cg']['claim'][0] = temp_auth

        json_struct['RoleInfo']['authorization'] = auth
        #print(json_struct)
        json_dict[excel_data_df['RoleName'][ind]] = json_struct

json_list = list(json_dict.values())
print("records : " + str(len(json_list)))
# print(type(json_list))
# print(json_dict)
json_array = sj.dumps(json_list, ignore_nan=True, encoding="utf-8")
# role_data = json.loads(json_array)
# role_data = json.dumps(json_array)
# print(type(role_data))
# for role in role_data:
#     print(role)
# print(data)
print(json_array)
