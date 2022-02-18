import json
import requests
import boto3
# import os
import sys
from backports.zoneinfo import ZoneInfo
from datetime import date, datetime, time
from datetime import datetime
# from datetime import timedelta  
# from boto3.dynamodb.conditions import Key, Attr
import random
import string
import time
from decimal import Decimal
import psycopg2

client = boto3.client('s3')
s3 = boto3.resource('s3')

headers = { 
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        }

def lambda_handler(event, context):
    if event['httpMethod'] == "POST":
        try:
            body=json.loads(event['body'])
            if "userid" not in body or "atoken" not in body or "appid" not in body or "domainid" not in body or 'orgid' not in body or "name" not in body or "leadtype" not in body or "mobile" not in body or "email" not in body or 'address1' not in body or 'address2' not in body or 'product' not in body or "post" not in body or "organization" not in body or 'currency' not in body or 'amount' not in body or 'note' not in body or 'date' not in body or 'status' not in body or 'filename' not in body or 'tmpfilename' not in body:
                body={
                    'returncode':"200",
                    'status':"Missing Field!"
                }
                return cb(200,body)
            else:
                con=connect()
                cursor=con.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS crmlead(autoid serial PRIMARY KEY, leadid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),leadtype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),product TEXT, post VARCHAR(255), organization VARCHAR(255), currency VARCHAR(20), amount VARCHAR(50),note VARCHAR(255), date VARCHAR(20), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),industrytype TEXT,leadsource TEXT)")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmproduct(autoid serial PRIMARY KEY,productid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),skucode varchar(20), name VARCHAR(255),price VARCHAR(50), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                con.commit()      
                userid = body['userid']
                atoken = body['atoken']
                appid = body['appid']  
                domainid = body['domainid']
                orgid = body['orgid']
                name = body['name'] 
                leadtype = body['leadtype']
                mobile = body['mobile']
                email = body['email']
                address1 = body['address1']
                address2 = body['address2']
                product = body['product']
                post = body['post']
                organization = body['organization']
                currency = body['currency']
                amount = body['amount']
                note = body['note']
                date = body['date']
                status = body['status']
                filename = body['filename']
                tmpfilename = body['tmpfilename']
                industrytype = ""
                leadsource = ""
                if 'industrytype' in body:
                    industrytype = body['industrytype']
                if 'leadsource' in body:
                    leadsource = body['leadsource']
                tokenreturn=checkAToken(userid,appid,atoken)
                if tokenreturn == "0":
                    timestamp =str(int(time.time()*1000.0))
                    leadid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))                  
                    sortbylocalFormat = "%Y%m%d%H%M%S"
                    now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                    sortby = now_asia.strftime(sortbylocalFormat)   
                    filedata = []
                   
                    localFormat = '%Y-%m-%d %H:%M:%S'  
                    if len(filename) > 0 and tmpfilename != "":
                        for i in range(len(filename)):
                            url = ""
                            copy_source = {
                                    'Bucket': 'kunyekbucket',
                                    'Key':'crm/'+ tmpfilename + '/' +filename[i]
                                }
                            client.copy(copy_source, 'kunyekbucket', 'crm/'+ leadid + '/' +filename[i])
                            s3.Object('kunyekbucket','crm/'+tmpfilename+ '/' +filename[i]).delete()  
                            # client.delete_object(Bucket="kunyekbucket", Key='crm/'+tmpfilename+ '/' +filename[i])  
                            expiretime = now_asia.strftime(localFormat)  
                            datetimenow = str(expiretime)                            
                            url = client.generate_presigned_url(
                                'get_object',
                                Params={
                                    'Bucket': 'kunyekbucket',
                                    'Key': 'crm/'+leadid + '/' + filename[i]
                                },
                                ExpiresIn = 1800,
                                HttpMethod='GET'
                            )
                            filedata.append({"filename":filename[i],"fileurl":url,"fileexp":datetimenow})
                            if len(filename)-1 == i:
                                s3.Object('kunyekbucket','crm/'+tmpfilename).delete()  
                                # client.delete_object(Bucket="kunyekbucket", Key='crm/'+tmpfilename)  
                    sql = "INSERT INTO crmlead(leadid,appid,domainid,orgid, name,leadtype,mobile,email,address1,address2,product , post , organization,currency,amount,note,date, status,userid,sortby,filename,industrytype,leadsource) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    data = (leadid,appid,domainid,orgid, name,leadtype, mobile, email, address1,address2,str(product), post, organization,currency,amount,note,date, status,userid,sortby,str(filedata),industrytype,leadsource)
                    cursor.execute(sql, data)
                    if len(product) > 0:
                        for i in range(len(product)):
                            seleprosql="SELECT * FROM crmproduct WHERE skucode=%s"
                            seleprodata=(product[i]['skucode'],)
                            cursor.execute(seleprosql,seleprodata)
                            productdata = cursor.fetchall()
                            if len(productdata) == 0: 
                                timestamp =str(int(time.time()*1000.0))
                                productid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))                  
                                sortby = now_asia.strftime(sortbylocalFormat)
                                prosql = "INSERT INTO crmproduct(productid,domainid,appid,orgid,userid,skucode, name,price,sortby) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                prodata = (productid,domainid,appid,orgid,userid,product[i]['skucode'],product[i]['name'],product[i]['price'],sortby)
                                cursor.execute(prosql, prodata)
                            else:
                                if productdata[0][7] != product[i]['name'] or productdata[0][8] != product[i]['price']:
                                    updatesql = "UPDATE crmproduct SET name=%s ,price=%s WHERE productid=%s"
                                    updatedata = (product[i]['name'],product[i]['price'],productdata[0][1])
                                    cursor.execute(updatesql, updatedata)
                    con.commit()
                    returndate = ""
                    if date != "":
                        if str(date)[4:5] == "-":
                            tyear = str(date)[0:4]
                            tmonth = str(date)[5:7]
                            tday = str(date)[8:10]
                            returndate = tday+"/"+tmonth+"/"+tyear
                        else:
                            returndate= date
                    data = {
                        'leadid':leadid,
                        'orgid': orgid,
                        'name': name,
                        'leadtype':leadtype,
                        'mobile': mobile,
                        'email': email,
                        'address1':address1,
                        'address2':address2,
                        'product':product,
                        'post':post,
                        'organization': organization,
                        'currency':currency,
                        'amount':amount,
                        'note':note,
                        'date':returndate,
                        'status': status,
                        'userid': userid,
                        'filelist': filedata,
                        'filename':"",
                        'fileurl':"",
                        'sort': str(sortby)
                    }
                    response = {
                        'returncode':"300",
                        'data' : data,
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
