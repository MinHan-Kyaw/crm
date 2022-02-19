import base64
import boto3

import requests
import json
import sys
from botocore.exceptions import ClientError
import psycopg2
# from
headers = {
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
}

def GetBucketSecret():
    bucketsecret = get_secret("dev/kunyek")
    data = json.loads(bucketsecret)
    if "access_id" in data and "secret_key" in data:
        return data
    else:
        return {
            "access_id": "",
            "secret_key": ""
        }

def GetKunyekToken():
    tokendata = get_secret("token/kunyek")
    tokendata = json.loads(tokendata)
    if 'token' in tokendata:
        return tokendata
    else:
        return {
            "token":""
        }

def GetCheckInToken():
    tokendata = get_secret("token/checkin")
    tokendata = json.loads(tokendata)
    if 'token' in tokendata:
        return tokendata
    else:
        return {
            "token":""
        }

def GetTokenSecret():   
    secret = get_secret("dev/iam")
    secret = json.loads(secret)
    if "secret_key" in secret:
        return secret
    else:
        return {
            "secret_key": ""
        }


def connect():
    dbinfo = get_secret(
        "arn:aws:secretsmanager:ap-southeast-1:983072393601:secret:dev/crm-GVgSv0")
    if 'username' in dbinfo and 'password' in dbinfo and 'host' in dbinfo and 'port' in dbinfo and 'dbInstanceIdentifier' in dbinfo:
        con = psycopg2.connect(dbname=dbinfo['dbInstanceIdentifier'], user=dbinfo['username'],
                               host=dbinfo['host'], password=dbinfo['password'], port=str(dbinfo['port']))
        return con
    else:
        return {
            "error": "db connect error"
        }


def checkAToken(userid, appid, atoken):
    headers = {'Content-type': 'application/json', 'Accept': '*/*'}
    params = {
        "userid": userid,
        "appid": appid,
        "atoken": atoken
    }
    result = requests.post(
        url="https://api1.iam.omnicloudapi.com/auth/checktoken", json=params, headers=headers)
    response_data = result.json()
    return response_data["returncode"]


def get_secret(SecretName):
    try:
        secret_name = SecretName
        region_name = "ap-southeast-1"
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            decoded_binary_secret = ""
            secret = ""
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                decoded_binary_secret = base64.b64decode(
                    get_secret_value_response['SecretBinary'])
            return secret
        except ClientError as e:
            response = {
                'returncode': '200',
                "status": "Server Error",
                "error": '{} error on line {}'.format(e, sys.exc_info()[-1].tb_lineno)
            }
            return response
    except Exception as e:
        response = {
            'returncode': '200',
            "status": "Server Error",
            "error": '{} error on line {}'.format(e, sys.exc_info()[-1].tb_lineno)
        }
        return response
