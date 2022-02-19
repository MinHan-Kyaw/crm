import json
import requests
import boto3
import os
import sys
from backports.zoneinfo import ZoneInfo
from datetime import date, datetime, time
from datetime import datetime
from datetime import timedelta
from boto3.dynamodb.conditions import Key, Attr
import random
import string
import time
# from urllib.parse import urlencode
# from urllib.request import urlopen
from decimal import Decimal
import common

dynamodb = boto3.resource('dynamodb')
s3value = common.GetBucketSecret()
ACCESS_ID = s3value['access_id']
SECRET_KEY = s3value['secret_key']
s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key=SECRET_KEY)
client = boto3.client('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key=SECRET_KEY)
userorgTable = dynamodb.Table('UserOrganizations')


headers = {
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
}


def lambda_handler(event, context):
    if event['httpMethod'] == "POST":
        try:
            body = json.loads(event['body'])
            if "userid" not in body or "atoken" not in body or "appid" not in body or "domainid" not in body or "leadid" not in body or 'orgid' not in body:
                body = {
                    'returncode': "200",
                    'status': "Missing Field!"
                }
                return cb(200, body)
            else:
                # con = common.connect()
                con = common.connect()
                cursor = con.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS crmlead(autoid serial PRIMARY KEY, leadid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),leadtype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),product TEXT, post VARCHAR(255), organization VARCHAR(255), currency VARCHAR(20), amount VARCHAR(50),note VARCHAR(255), date VARCHAR(20), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),industrytype TEXT)")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcustomer(autoid serial PRIMARY KEY, customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),customertype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),industrytype TEXT)")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcontact(autoid serial PRIMARY KEY,contactid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),post VARCHAR(255), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmdeal(autoid serial PRIMARY KEY, dealid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), targetdate Date, closingdate Date, currency VARCHAR(20), amount VARCHAR(50), product TEXT, status varchar(20), stageid varchar(20), remark TEXT, sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),salecode VARCHAR(50),opendate Date)")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmproduct(autoid serial PRIMARY KEY,productid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),skucode varchar(20), name VARCHAR(255), price varchar(50),sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                con.commit()
                userid = body['userid']
                atoken = body['atoken']
                appid = body['appid']
                domainid = body['domainid']
                orgid = body['orgid']
                leadid = body['leadid']
                leadtype = ""
                customername = ""
                if 'leadtype' in body:
                    leadtype = body['leadtype']
                if 'organization' in body:
                    customername = body['organization']
                tokenreturn = checkAToken(userid, appid, atoken)
                if tokenreturn == "0":
                    userres = userorgTable.scan(
                        FilterExpression=Attr('domainid').eq(domainid) & Attr('orgid').eq(
                            orgid) & Attr('t1').eq('300') & Attr('userid').eq(userid)
                    )
                    if len(userres['Items']) == 0:
                        sql = "SELECT * FROM crmlead WHERE leadid=%s AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
                        data = (leadid, domainid, userid, appid, orgid)
                        cursor.execute(sql, data)
                        leadlist = cursor.fetchall()
                    else:
                        sql = "SELECT * FROM crmlead WHERE leadid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                        data = (leadid, domainid, appid, orgid)
                        cursor.execute(sql, data)
                        leadlist = cursor.fetchall()
                    if len(leadlist) == 0:
                        response = {
                            'returncode': "200",
                            'status': "Invalid Lead!"
                        }
                        return cb(200, response)
                    else:
                        leadid = leadlist[0][1]
                        name = leadlist[0][6]
                        if leadtype == "":
                            leadtype = leadlist[0][7]
                        if leadtype == "001":
                            if customername == "":
                                customername = leadlist[0][14]
                        else:
                            customername = name
                        post = leadlist[0][13]
                        mobile = leadlist[0][8]
                        email = leadlist[0][9]
                        address1 = leadlist[0][10]
                        address2 = leadlist[0][11]
                        currency = leadlist[0][15]
                        amount = leadlist[0][16]
                        date = leadlist[0][18]
                        if leadlist[0][32] == None:
                            industrytype = ""
                        else:
                            industrytype = leadlist[0][32]
                        product = []
                        filename = []
                        if leadlist[0][12] != "":
                            product = eval(leadlist[0][12])
                        if leadlist[0][21] != "" and len(eval(leadlist[0][21])) > 0:
                            filename = eval(leadlist[0][21])
                        status = "002"
                        timestamp = str(int(time.time()*1000.0))
                        customerid = str(
                            ''.join([random.choice(string.digits+timestamp) for n in range(10)]))
                        timestamp = str(int(time.time()*1000.0))
                        contactid = str(
                            ''.join([random.choice(string.digits+timestamp) for n in range(10)]))
                        timestamp = str(int(time.time()*1000.0))
                        dealid = str(
                            ''.join([random.choice(string.digits+timestamp) for n in range(10)]))
                        # timestamp =str(int(time.time()*1000.0))
                        # productid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))
                        localFormat = "%Y%m%d%H%M%S"
                        dateFormat = "%Y-%m-%d"
                        now_asia = datetime.now(
                            ZoneInfo("Asia/Yangon")) + timedelta(10)
                        targetdate = now_asia.strftime(dateFormat)
                        now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                        sortby = now_asia.strftime(localFormat)
                        todaydate = now_asia.strftime(dateFormat)
                        sql = "INSERT INTO crmcustomer(customerid,domainid,appid,orgid,userid ,name , customertype,mobile,email,address1,address2,status,sortby,filename,industrytype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        data = (customerid, domainid, appid, orgid, userid, customername, leadtype,
                                mobile, email, address1, address2, status, sortby, str(filename),industrytype)
                        cursor.execute(sql, data)
                        consql = "INSERT INTO crmcontact(contactid, customerid,domainid,appid,orgid,userid ,name , post,mobile,email,address1,address2,sortby) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        condata = (contactid, customerid, domainid, appid, orgid, userid,
                                   name, post, mobile, email, address1, address2, sortby)
                        cursor.execute(consql, condata)
                        if len(product) > 0 or amount != "":
                            dealsql = "INSERT INTO crmdeal(dealid,customerid,domainid,appid,orgid,userid,product, targetdate, closingdate,currency,amount,status,stageid,remark,sortby,opendate,salecode) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            dealdata = (dealid, customerid, domainid, appid, orgid, userid, str(
                                product), targetdate, targetdate, currency, amount, "001", "1898531130", "", sortby, todaydate, "")
                            cursor.execute(dealsql, dealdata)
                        if len(filename) > 0:
                            for f in range(len(filename)):
                                copy_source = {
                                    'Bucket': 'kunyekbucket',
                                    'Key': 'crm/' + leadid + '/' + filename[f]['filename']
                                }
                                client.copy(
                                    copy_source, 'kunyekbucket', 'crm/' + customerid + '/' + filename[f]['filename'])
                                s3.Object('kunyekbucket','crm/'+leadid+'/'+filename[f]['filename']).delete()  
                                # client.delete_object(
                                #     Bucket="kunyekbucket", Key='crm/'+leadid+'/'+filename[f]['filename'])
                                if len(filename) == f + 1:
                                    s3.Object('kunyekbucket','crm/'+leadid).delete()  
                                    # client.delete_object(
                                    #     Bucket="kunyekbucket", Key='crm/'+leadid)
                        leadsql = "UPDATE crmlead SET status=%s WHERE leadid=%s AND domainid=%s AND orgid=%s"
                        leaddata = ("001", leadid, domainid, orgid)
                        cursor.execute(leadsql, leaddata)
                        con.commit()
                        response = {
                            'returncode': "300",
                            'customerid': customerid,
                            'status': "Converted Successfully!"
                        }
                        return cb(200, response)
                elif tokenreturn == "1":
                    response = {
                        'returncode': "210",
                        "status": "Invalid Token"
                    }
                    return cb(200, response)
                elif tokenreturn == "3":
                    response = {
                        'returncode': "210",
                        "status": "Token Error"
                    }
                    return cb(200, response)

        except Exception as e:
            response = {
                'returncode': '200',
                "status": "Server Error",
                "error": '{} error on line {}'.format(e, sys.exc_info()[-1].tb_lineno)
            }
            return cb(200, response)


# def connect():
#     con = psycopg2.connect(dbname="crmdb", user="crmuser",
#                            host="crmdb.cidwusqgeeug.ap-southeast-1.rds.amazonaws.com", password="Nirvasoft1234", port="5432")
#     return con


def default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Object of type '%s' is not JSON serializable" %
                    type(obj).__name__)


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
    # return "0"
    return response_data["returncode"]


def cb(statuscode, body):
    return {
        'statusCode': int(statuscode),
        'headers': headers,
        'body': json.dumps(body, default=default)
    }


def cb1(statuscode, body):
    return {
        'statusCode': int(statuscode),
        'headers': headers,
        'body': body
    }
