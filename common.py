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
            "token": ""
        }


def GetCheckInToken():
    tokendata = get_secret("token/checkin")
    tokendata = json.loads(tokendata)
    if 'token' in tokendata:
        return tokendata
    else:
        return {
            "token": ""
        }


def CreateAllDB(con):
    cursor = con.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS crmlead(autoid serial PRIMARY KEY, leadid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),leadtype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),product TEXT, post VARCHAR(255), organization VARCHAR(255), currency VARCHAR(20), amount VARCHAR(50),note VARCHAR(255), date VARCHAR(20), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
    cursor.execute("CREATE TABLE IF NOT EXISTS crmcustomer(autoid serial PRIMARY KEY, customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),customertype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
    cursor.execute("CREATE TABLE IF NOT EXISTS crmcontact(autoid serial PRIMARY KEY,contactid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),post VARCHAR(255), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
    cursor.execute("CREATE TABLE IF NOT EXISTS crmdeal(autoid serial PRIMARY KEY, dealid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), targetdate Date, closingdate Date, currency VARCHAR(20), amount VARCHAR(50), product TEXT, status varchar(20), stageid varchar(20), remark TEXT, sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),salecode VARCHAR(50),opendate Date)")
    cursor.execute("CREATE TABLE IF NOT EXISTS crmproduct(autoid serial PRIMARY KEY,productid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),skucode varchar(20), name VARCHAR(255),price varchar(50), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
    cursor.execute("CREATE TABLE IF NOT EXISTS crmsalecode(autoid serial PRIMARY KEY,salecodeid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),salecode varchar(50), sortby VARCHAR(50))")
    cursor.execute("CREATE TABLE IF NOT EXISTS crmdealstage(autoid serial PRIMARY KEY, stageid VARCHAR(10), name varchar(50),domainid varchar(20),appid varchar(20),sort varchar(10))")
    con.commit()


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
        "arn:aws:secretsmanager:ap-southeast-1:983072393601:secret:crm/db-YUVKWZ")
    dbinfo = json.loads(dbinfo)
    if 'username' in dbinfo and 'password' in dbinfo and 'host' in dbinfo and 'port' in dbinfo and 'dbInstanceIdentifier' in dbinfo:
        con = psycopg2.connect(dbname=dbinfo['dbInstanceIdentifier'], user=dbinfo['username'],
                               host=dbinfo['host'], password=dbinfo['password'], port=str(dbinfo['port']))

        CreateAllDB(con)
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


def resetSerialNumber(tbName):
    con = connect()
    cursor = con.cursor()
    cursor.execute("select setval(%s, (select max(autoid) from " +
                   tbName+" ))", ((tbName+"_autoid_seq"),))
    con.commit()


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
