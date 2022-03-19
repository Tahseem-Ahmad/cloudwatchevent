import json
import io as io
import boto3
import pandas as pd
import openpyxl
import simplejson as sj
import urllib.parse

print('Loading function')

s3 = boto3.client('s3')
dynamoDb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    print("BUCKET : " + bucket)
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("KEY : " + key)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        excel_data_df = pd.read_excel(io.BytesIO(response['Body'].read()), dtype=str)
        # print(excel_data_df)
        json_array = convertExcelRowToJson(excel_data_df)
        return json_array
    except Exception as e:
        print(e)
        raise e


def convertExcelRowToJson(excel_data_df):
    json_list = []
    try:
        for ind in excel_data_df.index:
            print(excel_data_df['RoleName'][ind])
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
            auth = {auth: {"fg_groups": [excel_data_df['FineGrainedGroup'][ind]],
                           "ndw_plan_code": [int(excel_data_df['NDWPlanCode'][ind])]}}
            json_struct['RoleInfo']['authorization'] = auth
            json_list.append(json_struct)

        # print(json_list)
        json_array = sj.dumps(json_list, ignore_nan=True, encoding="utf-8")
        print(json_array)
        pushDataToTable(json_array)
        return json_array
    except Exception as e:
        # print(e)
        # print("Exception while converting dataframe to json")
        raise e


def dateToStr(param):
    if type(param) == str:
        yyyy, mm, dd = param.split(" ")[0].split("-")[0], param.split(" ")[0].split("-")[1], \
                       param.split(" ")[0].split("-")[2]
        return dd + '/' + mm + '/' + yyyy


def pushDataToTable(json_array):
    try:
        role_data = json.loads(json_array)
        table = dynamoDb.Table('USERS')
        with table.batch_writer(overwrite_by_pkeys=['RoleName']) as batch:
            for item in role_data:
                batch.put_item(Item=item)
    except Exception as e:
        raise e
