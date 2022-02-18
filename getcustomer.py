import json
import requests
# import os
import boto3
import sys
# from backports.zoneinfo import ZoneInfo
from datetime import date, datetime, time
from datetime import datetime
# from datetime import timedelta  
from backports.zoneinfo import ZoneInfo
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
            # header=event['headers']
            # btoken=header['btoken']
            # puserid=header['puserid']
            # pappid=header['pappid']
            if "userid" not in body or "atoken" not in body or "appid" not in body or "domainid" not in body or 'orgid' not in body:
                body={
                    'returncode':"200",
                    'status':"Missing Field!"
                }
                return cb(200,body)
            else:
                con=connect()
                cursor=con.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS crmlead(autoid serial PRIMARY KEY, leadid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),leadtype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),product TEXT, post VARCHAR(255), organization VARCHAR(255), currency VARCHAR(20), amount VARCHAR(50),note VARCHAR(255), date VARCHAR(20), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),industrytype TEXT)")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcustomer(autoid serial PRIMARY KEY, customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),customertype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),industrytype TEXT)")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcontact(autoid serial PRIMARY KEY,contactid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),post VARCHAR(255), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmdeal(autoid serial PRIMARY KEY, dealid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), targetdate Date, closingdate Date, currency VARCHAR(20), amount VARCHAR(50), product TEXT, status varchar(20), stageid varchar(20), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),salecode VARCHAR(50),opendate Date)")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmproduct(autoid serial PRIMARY KEY,productid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),skucode varchar(20), name VARCHAR(255), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmsalecode(autoid serial PRIMARY KEY,salecodeid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),salecode varchar(20), sortby VARCHAR(50))")
                con.commit()         
                userid = body['userid']
                atoken = body['atoken']
                appid = body['appid']  
                domainid = body['domainid']
                orgid = body['orgid']
                # btoken=_json['btoken']
                # puserid=_json['puserid']
                # pappid=_json['pappid']
                tokenreturn=checkAToken(userid,appid,atoken)
                if tokenreturn == "0":
                    sql="SELECT * FROM crmcustomer WHERE domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
                    data=(domainid,userid,appid,orgid)
                    cursor.execute(sql,data)
                    customerlist = cursor.fetchall()
                    if len(customerlist) == 0: 
                        response = {
                            'returncode': "300" ,
                            'list': []
                        }
                        return cb(200,response)
                    else:   
                        salecodesql="SELECT * FROM crmsalecode"
                        salecodedata=() 
                        cursor.execute(salecodesql,salecodedata)
                        salecodelist = cursor.fetchall()
                        reObj=dict()
                        customerlistreturn=list()            
                        for i in range(len(customerlist)):   
                            consql="SELECT * FROM crmcontact WHERE customerid=%s AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
                            condata=(customerlist[i][1],domainid,userid,appid,orgid)
                            cursor.execute(consql,condata)
                            contactlist =cursor.fetchall()
                            contactreObj = dict()
                            contactlistreturn = [] 
                            if len(contactlist) > 0:
                                for c in range(len(contactlist)):
                                    contactreObj['contactname'] = contactlist[c][7]
                                    contactreObj['contactid'] = contactlist[c][1]
                                    contactreObj['contactpost'] = contactlist[c][8]
                                    contactreObj['contactmobile'] = contactlist[c][9]
                                    contactreObj['contactemail'] = contactlist[c][10]
                                    contactreObj['contactaddress1'] = contactlist[c][11]
                                    contactreObj['contactaddress2'] = contactlist[c][12]
                                    contactreObj_copy=contactreObj.copy()
                                    contactlistreturn.append(contactreObj_copy)
                            dealsql="SELECT * FROM crmdeal WHERE customerid=%s AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
                            dealdata=(customerlist[i][1],domainid,userid,appid,orgid)
                            cursor.execute(dealsql,dealdata)
                            deallist =cursor.fetchall()
                            dealreObj = dict()
                            deallistreturn = [] 
                            stagesql="SELECT * FROM crmdealstage"
                            stagedata=() 
                            cursor.execute(stagesql,stagedata)
                            stagelist = cursor.fetchall()
                            if len(deallist) > 0:
                                for d in range(len(deallist)):
                                    dealreObj['dealid'] = deallist[d][1]
                                    if deallist[d][7] != "":
                                        tyear = str(deallist[d][7])[0:4]
                                        tmonth = str(deallist[d][7])[5:7]
                                        tday = str(deallist[d][7])[8:10]
                                        dealreObj['targetdate'] = tday+"/"+tmonth+"/"+tyear
                                    else:
                                        dealreObj['targetdate'] = ""
                                    if deallist[d][8] != "":
                                        cyear = str(deallist[d][8])[0:4]
                                        cmonth = str(deallist[d][8])[5:7]
                                        cday = str(deallist[d][8])[8:10]
                                        dealreObj['closingdate'] = cday+"/"+cmonth+"/"+cyear
                                    else:
                                        dealreObj['closingdate'] = ""
                                    # dealreObj['targetdate'] = deallist[d][7]
                                    # dealreObj['closingdate'] = deallist[d][8]
                                    filtersalecodeid = list(filter(lambda x: x[1] ==  deallist[d][26], salecodelist))
                                    if len(filtersalecodeid) > 0:    
                                        dealreObj['salecode'] =filtersalecodeid[0][6]
                                    else:
                                        dealreObj['salecode'] =deallist[d][26]
                                    if deallist[d][27] != None:
                                        oyear = str(deallist[d][27])[0:4]
                                        omonth = str(deallist[d][27])[5:7]
                                        oday = str(deallist[d][27])[8:10]
                                        dealreObj['opendate'] = oday+"/"+omonth+"/"+oyear
                                    else:
                                        dealreObj['opendate'] = ""
                                    dealreObj['currency'] = deallist[d][9]
                                    dealreObj['amount'] = deallist[d][10]
                                    dealreObj['status'] = deallist[d][12]
                                    filterstageid = list(filter(lambda x: x[1] ==  deallist[d][13], stagelist))
                                    dealreObj['stageid'] = deallist[d][13]
                                    dealreObj['remark'] = deallist[d][14]
                                    if len(filterstageid) > 0:
                                        dealreObj['stagename'] = filterstageid[0][2]
                                    else:
                                        dealreObj['stagename'] = ""
                                    dealreObj['product'] = eval(deallist[d][11])
                                    dealreObj_copy=dealreObj.copy()
                                    deallistreturn.append(dealreObj_copy)
                            reObj['customerid'] = customerlist[i][1]
                            reObj['customername'] = customerlist[i][6]
                            reObj['customertype'] = customerlist[i][7]
                            reObj['customermobile'] = customerlist[i][8]
                            reObj['customeremail'] = customerlist[i][9]
                            reObj['customeraddress1'] = customerlist[i][10]
                            reObj['customeraddress2'] = customerlist[i][11]
                            reObj['customerstatus'] = customerlist[i][12]
                            if customerlist[i][25] == None:
                                reObj['industrytype'] = ""
                            else:    
                                reObj['industrytype'] = customerlist[i][25]
                            filedata = []
                            if customerlist[i][14] != "":
                                customerlistdata = eval(customerlist[i][14])
                                toupdate = "false"
                                for f in range(len(customerlistdata)):
                                    fileurl = ""
                                    localFormat = '%Y-%m-%d %H:%M:%S'
                                    now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                                    expiretime = now_asia.strftime(localFormat)  
                                    datetimenow = str(expiretime)
                                    expire2 = datetime.strptime(datetimenow, localFormat)
                                    datetimedata = customerlistdata[f]['fileexp']
                                    expire = datetime.strptime(datetimedata, localFormat)
                                    # expire2 = datetime.strptime(datetimenow, localFormat)
                                    minutes_diff = (expire2 - expire).total_seconds() / 60.0
                                    if int(minutes_diff) >= 30:
                                        toupdate = "true"
                                        fileurl = client.generate_presigned_url(
                                                'get_object',
                                                Params={
                                                    'Bucket': 'kunyekbucket',
                                                    'Key': 'crm/'+customerlist[i][1] + '/' + customerlistdata[f]['filename']
                                                },
                                                ExpiresIn = 1800,
                                                HttpMethod='GET'
                                            )
                                        filedata.append({"filename":customerlistdata[f]['filename'],"fileurl":fileurl,"fileexp":datetimenow})               
                                    else:
                                        filedata.append(customerlistdata[f])
                                        # fileurl = eval(customerlist[i][14])['fileurl']
                                if toupdate == "true":
                                    leadsql = "UPDATE crmcustomer SET filename=%s WHERE customerid=%s AND domainid=%s AND userid=%s"
                                    leaddata = (str(filedata), customerlist[i][1], domainid,userid)
                                    cursor.execute(leadsql, leaddata)
                                    con.commit()
                            reObj['filelist'] = filedata
                            reObj['filename'] = ""
                            reObj['fileurl'] = ""
                            reObj['contactlist'] = contactlistreturn
                            reObj['deallist'] = deallistreturn
                            reObj_copy=reObj.copy()
                            customerlistreturn.append(reObj_copy)
                        response = {
                            'returncode':"300",
                            'list' : customerlistreturn,
                            'total': len(customerlistreturn),
                            'status': "Customer List!"
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
            'body': body
    }
