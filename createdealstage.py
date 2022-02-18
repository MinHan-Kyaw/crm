import json
import requests
# import boto3
# import os
import sys
# from backports.zoneinfo import ZoneInfo
from datetime import date, datetime, time
from datetime import datetime
# from datetime import timedelta  
# from boto3.dynamodb.conditions import Key, Attr
import random
import string
import time
from decimal import Decimal
import psycopg2


headers = { 
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        }

def lambda_handler(event, context):
    if event['httpMethod'] == "POST":
        try:
            body=json.loads(event['body'])
            if "userid" not in body or "atoken" not in body or "appid" not in body or "domainid" not in body:
                body={
                    'returncode':"200",
                    'status':"Missing Field!"
                }
                return cb(200,body)
            else:
                con=connect()
                cursor=con.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS crmdealstage(autoid serial PRIMARY KEY, stageid VARCHAR(10), name varchar(50),domainid varchar(20),appid varchar(20),sort varchar(10))")
                con.commit()      
                userid = body['userid']
                atoken = body['atoken']
                appid = body['appid']  
                domainid = body['domainid']
                name = body['name'] 
                sort = body['sort']
                tokenreturn=checkAToken(userid,appid,atoken)
                if tokenreturn == "0":
                    timestamp =str(int(time.time()*1000.0))
                    stageid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))                  
                    sql = "INSERT INTO crmdealstage(stageid,appid,domainid, name,sort) VALUES(%s,%s,%s,%s,%s)"
                    data = (stageid,appid,domainid, name,sort)
                    cursor.execute(sql, data)
                    con.commit()
                    response = {
                        'returncode':"300",
                        'status': "Created Successfully!"
                    } 
                    return cb(200,response)                            
                elif tokenreturn == "1":
                    response = {
                        'returncode':"210",
                        "status":"Invalid Token"
                    }
                    return cb(200,response)    
                elif tokenreturn == "3":
                    response = {
                        'returncode':"210",
                        "status":"Token Error"
                    }
                    return cb(200,response)   

        except Exception as e:
            response={
                    'returncode':'200',
                    "status":"Server Error",
                    "error":'{} error on line {}'.format(e,sys.exc_info()[-1].tb_lineno)
                }
            return cb(200,response)


def connect():
    con=psycopg2.connect(dbname="crmdb", user="crmuser",host="crmdb.cidwusqgeeug.ap-southeast-1.rds.amazonaws.com", password="Nirvasoft1234",port="5432")
    return con


def default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)

def checkAToken(userid,appid,atoken):
        headers={'Content-type':'application/json', 'Accept':'*/*'}
        params = {
            "userid": userid,
            "appid": appid,
            "atoken": atoken
        }
        result=requests.post(url="https://api1.iam.omnicloudapi.com/auth/checktoken",json=params,headers=headers)
        response_data = result.json()
        # return "0"
        return response_data["returncode"]
        
        
def cb(statuscode,body):
    return {
            'statusCode': int(statuscode),
            'headers': headers,
            'body': json.dumps(body, default=default)
    }

def cb1(statuscode,body):
    return {
            'statusCode': int(statuscode),
            'headers': headers,
            'body': str(body)
    }
