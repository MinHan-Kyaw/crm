import json
import requests
# import os
import boto3
import sys
from backports.zoneinfo import ZoneInfo
from datetime import date, datetime, time
from datetime import datetime
from datetime import timedelta  
import random
import string
import time
from decimal import Decimal

import common

s3value = common.GetBucketSecret()
ACCESS_ID = s3value['access_id']
SECRET_KEY = s3value['secret_key']
client = boto3.client('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key=SECRET_KEY)
s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key=SECRET_KEY)

headers = { 
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        }

def lambda_handler(event, context):
    if event['httpMethod'] == "POST":
        try:
            body=json.loads(event['body'])
            if "userid" not in body or "atoken" not in body or "appid" not in body or "domainid" not in body or "orgid" not in body or "customername" not in body or "customertype" not in body or "customermobile" not in body or "customeremail" not in body or "customeraddress1" not in body or "customeraddress2" not in body or "status" not in body or "filename" not in body or "tmpfilename" not in body or "deallist" not in body:
                body={
                    'returncode':"200",
                    'status':"Missing Field!"
                }
                return cb(200,body)
            else:
                con = common.connect()
                cursor=con.cursor()
                # cursor.execute("CREATE TABLE IF NOT EXISTS crmlead(autoid serial PRIMARY KEY, leadid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),leadtype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),product TEXT, post VARCHAR(255), organization VARCHAR(255), currency VARCHAR(20), amount VARCHAR(50),note VARCHAR(255), date VARCHAR(20), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),industrytype TEXT)")
                # cursor.execute("CREATE TABLE IF NOT EXISTS crmcustomer(autoid serial PRIMARY KEY, customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),customertype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),industrytype TEXT)")
                # cursor.execute("CREATE TABLE IF NOT EXISTS crmcontact(autoid serial PRIMARY KEY,contactid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),post VARCHAR(255), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                # cursor.execute("CREATE TABLE IF NOT EXISTS crmdeal(autoid serial PRIMARY KEY, dealid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), targetdate Date, closingdate Date, currency VARCHAR(20), amount VARCHAR(50), product TEXT, status varchar(20), stageid varchar(20), remark TEXT, sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),salecode VARCHAR(50),opendate Date)")
                # cursor.execute("CREATE TABLE IF NOT EXISTS crmproduct(autoid serial PRIMARY KEY,productid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),skucode varchar(20), name VARCHAR(255),price varchar(50), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                # con.commit()      
                userid = body['userid']
                atoken = body['atoken']
                appid = body['appid']  
                domainid = body['domainid'] 
                orgid = body['orgid']
                customername = body['customername']
                customertype = body['customertype']
                customermobile = body['customermobile']
                customeremail = body['customeremail']
                customeraddress1 = body['customeraddress1']
                customeraddress2 = body['customeraddress2']
                status = body['status']
                filename = body['filename']
                tmpfilename = body['tmpfilename']
                # contactname = body['contactname']
                # post = body['post']
                contactlist = []
                if 'contactlist' in body:
                    contactlist = body['contactlist']
                deallist = []
                if 'deallist' in body:
                    deallist = body['deallist']
                industrytype = ""
                if 'industrytype' in body:
                    industrytype = body['industrytype']
                # contactmobile = body['contactmobile']
                # contactemail = body['contactemail']
                # contactaddress1 = body['contactaddress1']
                # contactaddress2 = body['contactaddress2']
                # product = body['product']
                # date = body['date']
                # targetdate = body['targetdate']
                # closingdate = body['closingdate']
                # stageid = body['stageid']
                # dealstatus = body['dealstatus']
                # currency = body['currency']
                # amount = body['amount']
                dealid = ""
                tokenreturn=checkAToken(userid,appid,atoken)
                if tokenreturn == "0":
                    timestamp =str(int(time.time()*1000.0))
                    contactid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))                  
                    timestamp =str(int(time.time()*1000.0))
                    customerid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))                  
                                   
                    localFormat = '%Y-%m-%d %H:%M:%S'
                    sortbylocalFormat = "%Y%m%d%H%M%S"
                    now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                    sortby = now_asia.strftime(sortbylocalFormat) 
                    filedata = []
                    fileurl = ""
                    bucket = s3.Bucket('kunyekbucket')
                    if len(filename) > 0 and tmpfilename != "":
                        for f in range(len(filename)):
                            fileurl = ""
                            copy_source = {
                                    'Bucket': 'kunyekbucket',
                                    'Key':'crm/'+ tmpfilename + '/' +filename[f]
                                }
                            client.copy(copy_source, 'kunyekbucket', 'crm/'+ customerid + '/' +filename[f])
                            bucket.objects.filter(Prefix='crm/'+tmpfilename+ '/' +filename[f]).delete()
                            # s3.Object('kunyekbucket','crm/'+tmpfilename+ '/' +filename[f]).delete()  
                            # client.delete_object(Bucket="kunyekbucket", Key='crm/'+tmpfilename+ '/' +filename[f])  
                            expiretime = now_asia.strftime(localFormat)  
                            datetimenow = str(expiretime)                            
                            fileurl = client.generate_presigned_url(
                                'get_object',
                                Params={
                                    'Bucket': 'kunyekbucket',
                                    'Key': 'crm/'+customerid + '/' + filename[f]
                                },
                                ExpiresIn = 1800,
                                HttpMethod='GET'
                            )
                            filedata.append({"filename":filename[f],"fileurl":fileurl,"fileexp":datetimenow})
                        if len(filename) - 1 == f:
                            bucket.objects.filter(Prefix='crm/'+tmpfilename).delete()
                            # s3.Object('kunyekbucket','crm/'+tmpfilename).delete()  
                            # client.delete_object(Bucket="kunyekbucket", Key='crm/'+tmpfilename)  
                    common.resetSerialNumber("crmcustomer")
                    sql = "INSERT INTO crmcustomer(customerid,domainid,appid,orgid,userid ,name , customertype,mobile,email,address1,address2,status,sortby,filename,industrytype) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    data = (customerid,domainid,appid,orgid,userid, customername,customertype,customermobile,customeremail,customeraddress1,customeraddress2,status,sortby,str(filedata),industrytype)
                    cursor.execute(sql, data)  
                    # if contactname != "":
                    #     consql = "INSERT INTO crmcontact(contactid, customerid,domainid,appid,orgid,userid ,name , post,mobile,email,address1,address2,sortby) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    #     condata = (contactid,customerid,domainid,appid,orgid,userid, contactname,post,contactmobile,contactemail,contactaddress1,contactaddress2,sortby)
                    #     cursor.execute(consql, condata) 
                    
                    # if len(product) > 0:
                    deallistreturn = []
                    dateFormat = "%Y-%m-%d"
                    if len(deallist) > 0:
                        dealreObj = dict() 
                        for i in range(len(deallist)):
                            sortby = now_asia.strftime(sortbylocalFormat) 
                            timestamp =str(int(time.time()*1000.0))
                            if deallist[i]['closingdate'] == "":
                                deallist[i]['closingdate'] = deallist[i]['targetdate']
                            
                            opendate = now_asia.strftime(dateFormat)
                            salecode = ""
                            if 'salecode' in deallist[i]:
                                salecode = deallist[i]['salecode']                                
                            if 'opendate' in deallist[i]:
                                opendate = deallist[i]['opendate']
                            dealid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))  
                            common.resetSerialNumber("crmdeal")
                            dealsql = "INSERT INTO crmdeal(dealid,customerid,domainid,appid,orgid,userid,product, targetdate, closingdate,currency,amount,status,stageid,remark,sortby,opendate,salecode) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            dealdata = (dealid,customerid,domainid,appid,orgid,userid,str(deallist[i]['product']),deallist[i]['targetdate'],deallist[i]['closingdate'],deallist[i]['currency'],deallist[i]['amount'],deallist[i]['status'],deallist[i]['stageid'],deallist[i]['remark'],sortby,opendate,salecode)
                            oyear = str(opendate)[0:4]
                            omonth = str(opendate)[5:7]
                            oday = str(opendate)[8:10]
                            dealreObj['opendate'] = oday+"/"+omonth+"/"+oyear
                            dealreObj['salecode'] = salecode
                            dealreObj['dealid'] = dealid
                            if str(deallist[i]['targetdate'])[4:5] == "-":
                                tyear = str(deallist[i]['targetdate'])[0:4]
                                tmonth = str(deallist[i]['targetdate'])[5:7]
                                tday = str(deallist[i]['targetdate'])[8:10]
                                dealreObj['targetdate'] = tday+"/"+tmonth+"/"+tyear
                            else:
                                dealreObj['targetdate'] = deallist[i]['targetdate']
                            if str(deallist[i]['closingdate'])[4:5] == "-":
                                cyear = str(deallist[i]['closingdate'])[0:4]
                                cmonth = str(deallist[i]['closingdate'])[5:7]
                                cday = str(deallist[i]['closingdate'])[8:10]
                                dealreObj['closingdate'] = cday+"/"+cmonth+"/"+cyear
                            else:
                                if str(deallist[i]['closingdate']) == "":
                                    dealreObj['closingdate'] = tday+"/"+tmonth+"/"+tyear
                                else:
                                    dealreObj['closingdate'] = deallist[i]['closingdate']
                            # dealreObj['closingdate'] = deallist[i]['closingdate']
                            dealreObj['currency'] = deallist[i]['currency']
                            dealreObj['amount'] = deallist[i]['amount']
                            dealreObj['status'] = deallist[i]['status']
                            dealreObj['stageid'] = deallist[i]['stageid']
                            dealreObj['remark'] = deallist[i]['remark']
                            dealreObj['product'] = deallist[i]['product']
                            dealreObj_copy=dealreObj.copy()
                            deallistreturn.append(dealreObj_copy)
                            cursor.execute(dealsql, dealdata)
                            if len(deallist[i]['product']) > 0:
                                product = deallist[i]['product']
                                for ii in range(len(product)):
                                    seleprosql="SELECT * FROM crmproduct WHERE skucode=%s"
                                    seleprodata=(product[ii]['skucode'],)
                                    cursor.execute(seleprosql,seleprodata)
                                    productdata = cursor.fetchall()
                                    if len(productdata) == 0: 
                                        timestamp =str(int(time.time()*1000.0))
                                        productid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))                  
                                        sortby = now_asia.strftime(sortbylocalFormat)
                                        common.resetSerialNumber("crmproduct")
                                        prosql = "INSERT INTO crmproduct(productid,domainid,appid,orgid,userid,skucode, name,price,sortby) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                        prodata = (productid,domainid,appid,orgid,userid,product[ii]['skucode'],product[ii]['name'],product[ii]['price'],sortby)
                                        cursor.execute(prosql, prodata)
                                    else:
                                        if productdata[0][7] != product[ii]['name'] or productdata[0][8] != product[ii]['price']:
                                            updatesql = "UPDATE crmproduct SET name=%s,price=%s WHERE productid=%s"
                                            updatedata = (product[ii]['name'],product[ii]['price'],productdata[0][1])
                                            cursor.execute(updatesql, updatedata)
                    contactlistreturn = []
                    if len(contactlist) > 0:
                        contactreObj = dict() 
                        for i in range(len(contactlist)):
                            timestamp =str(int(time.time()*1000.0))
                            contactid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))                  
                            now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                            sortby = now_asia.strftime(sortbylocalFormat) 
                            common.resetSerialNumber("crmcontact")
                            consql = "INSERT INTO crmcontact(contactid, customerid,domainid,appid,orgid,userid ,name , post,mobile,email,address1,address2,sortby) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            condata = (contactid,customerid,domainid,appid,orgid,userid, contactlist[i]['contactname'],contactlist[i]['post'],contactlist[i]['contactmobile'],contactlist[i]['contactemail'],contactlist[i]['contactaddress1'],contactlist[i]['contactaddress2'],sortby)
                            contactreObj['contactname'] = contactlist[i]['contactname']
                            contactreObj['contactid'] = contactid
                            contactreObj['contactpost'] = contactlist[i]['post']
                            contactreObj['contactmobile'] = contactlist[i]['contactmobile']
                            contactreObj['contactemail'] = contactlist[i]['contactemail']
                            contactreObj['contactaddress1'] = contactlist[i]['contactaddress1']
                            contactreObj['contactaddress2'] = contactlist[i]['contactaddress2']
                            contactreObj_copy=contactreObj.copy()
                            contactlistreturn.append(contactreObj_copy)
                            cursor.execute(consql, condata) 
                    
                            # con.commit()
                    con.commit()
                    reObj=dict()
                    customerlistreturn=list()  
                    # if contactname != "":
                    #     contactreObj = dict()                         
                    #     contactlistreturn = []
                    #     contactreObj['contactname'] = contactname
                    #     contactreObj['contactid'] = contactid
                    #     contactreObj['contactpost'] = post
                    #     contactreObj['contactmobile'] = contactmobile
                    #     contactreObj['contactemail'] = contactemail
                    #     contactreObj['contactaddress1'] = contactaddress1
                    #     contactreObj['contactaddress2'] = contactaddress2
                    #     contactreObj_copy=contactreObj.copy()
                    #     contactlistreturn.append(contactreObj_copy)
                    # dealreObj = dict()
                    # deallistreturn = [] 
                    # dealreObj['dealid'] = dealid
                    # dealreObj['product'] = product
                    # dealreObj['date'] = date
                    # dealreObj['currency'] = currency
                    # dealreObj['amount'] = amount
                    # dealreObj_copy=dealreObj.copy()
                    # deallistreturn.append(dealreObj_copy)
                    reObj['customerid'] = customerid
                    reObj['customername'] = customername
                    reObj['customertype'] = customertype
                    reObj['customermobile'] = customermobile
                    reObj['customeremail'] = customeremail
                    reObj['customeraddress1'] = customeraddress1
                    reObj['customeraddress2'] = customeraddress2
                    reObj['customerstatus'] = status
                    reObj['filename'] = filename
                    reObj['fileurl'] = fileurl
                    reObj['filelist'] = filedata
                    reObj['contactlist'] = contactlistreturn
                    reObj['deallist'] = deallistreturn
                    reObj_copy=reObj.copy()
                    customerlistreturn.append(reObj_copy)
                    response = {
                        'returncode':"300",
                        'list' : customerlistreturn,
                        'total': len(customerlistreturn),
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
