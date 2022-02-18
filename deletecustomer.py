import json
import requests
# import os
import boto3
import sys
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import psycopg2
# import jwt

client = boto3.client('s3')
s3 = boto3.resource('s3')

headers = { 
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        }

dynamodb = boto3.resource('dynamodb')
userorgTable = dynamodb.Table('UserOrganizations')

def lambda_handler(event, context):
    if event['httpMethod'] == "POST":
        try:
            body=json.loads(event['body'])
            if "userid" not in body or "atoken" not in body or "appid" not in body or "orgid" not in body or "domainid" not in body or "customerid" not in body:
                body={
                    'returncode':"200",
                    'status':"Missing Field!"
                }
                return cb(200,body)
            else:
                con=connect()
                cursor=con.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS crmlead(autoid serial PRIMARY KEY, leadid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),leadtype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),product TEXT, post VARCHAR(255), organization VARCHAR(255), currency VARCHAR(20), amount VARCHAR(50),note VARCHAR(255), date VARCHAR(20), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcustomer(autoid serial PRIMARY KEY, customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),customertype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcontact(autoid serial PRIMARY KEY,contactid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),post VARCHAR(255), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmdeal(autoid serial PRIMARY KEY, dealid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), targetdate VARCHAR(20), closingdate VARCHAR(20), currency VARCHAR(20), amount VARCHAR(50), product TEXT, status varchar(20), stageid varchar(20), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmproduct(autoid serial PRIMARY KEY,productid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),skucode varchar(20), name VARCHAR(255), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                con.commit()         
                userid = body['userid']
                atoken = body['atoken']
                appid = body['appid']  
                customerid = body['customerid']
                domainid = body['domainid']
                orgid = body['orgid']
                tokenreturn=checkAToken(userid,appid,atoken)
                if tokenreturn == "0":
                    userres = userorgTable.scan(
                        FilterExpression=Attr('domainid').eq(domainid) & Attr('orgid').eq(
                            orgid) & Attr('t1').eq('300') & Attr('userid').eq(userid)
                    )
                    if len(userres['Items']) == 0:
                        sql="SELECT * FROM crmcustomer WHERE customerid=%s AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
                        data=(customerid,domainid,userid,appid,orgid)                    
                        cursor.execute(sql,data)
                        customerlist = cursor.fetchall()
                    else:
                        sql="SELECT * FROM crmcustomer WHERE customerid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                        data=(customerid,domainid,appid,orgid)                    
                        cursor.execute(sql,data)
                        customerlist = cursor.fetchall()
                    if len(customerlist) == 0: 
                        response = {
                            'returncode': "200" ,
                            'status': "Invalid Customer!"
                        }
                        return cb(200,response)
                    else:
                        deletecusql="DELETE FROM crmcustomer WHERE customerid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                        deletecudata=(customerid,domainid,appid,orgid)
                        cursor.execute(deletecusql,deletecudata)
                        if customerlist[0][14] != "":
                            s3.Object('kunyekbucket',"crm/" + customerid).delete()   
                        deleteconsql="DELETE FROM crmcontact WHERE customerid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                        deletecondata=(customerid,domainid,appid,orgid)
                        cursor.execute(deleteconsql,deletecondata)
                        deletedealsql="DELETE FROM crmdeal WHERE customerid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                        deletedealdata=(customerid,domainid,appid,orgid)
                        cursor.execute(deletedealsql,deletedealdata)
                        con.commit()
                        response= {
                            'returncode':"300",
                            'status' : "Deleted Successfully!"
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
