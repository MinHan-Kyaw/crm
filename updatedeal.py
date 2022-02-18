import json
import requests
import boto3
import sys
from backports.zoneinfo import ZoneInfo
from datetime import date, datetime, time
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import random
import string
import time
from decimal import Decimal
import psycopg2

dynamodb = boto3.resource('dynamodb')
userorgTable = dynamodb.Table('UserOrganizations')

headers = { 
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        }

def lambda_handler(event, context):
    if event['httpMethod'] == "POST":
        try:
            body=json.loads(event['body'])
            if "userid" not in body or "atoken" not in body or "appid" not in body or "domainid" not in body or "orgid" not in body or "dealid" not in body or "customerid" not in body or "product" not in body or "targetdate" not in body or "closingdate" not in body or "currency" not in body or "amount" not in body or "status" not in body or "stageid" not in body or 'remark' not in body:
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
                cursor.execute("CREATE TABLE IF NOT EXISTS crmdeal(autoid serial PRIMARY KEY, dealid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), targetdate Date, closingdate Date, currency VARCHAR(20), amount VARCHAR(50), product TEXT, status varchar(20), stageid varchar(20), remark TEXT, sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),salecode VARCHAR(50),opendate Date)")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmproduct(autoid serial PRIMARY KEY,productid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),skucode varchar(20), name VARCHAR(255),price varchar(50), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmsalecode(autoid serial PRIMARY KEY,salecodeid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),salecode varchar(20), sortby VARCHAR(50))")
                con.commit()           
                userid = body['userid']
                atoken = body['atoken']
                appid = body['appid']  
                domainid = body['domainid'] 
                orgid = body['orgid']
                customerid = body['customerid']
                status = body['status']
                product = body['product']
                targetdate = body['targetdate']
                closingdate = body['closingdate']
                currency = body['currency']
                amount = body['amount']
                stageid = body['stageid']
                remark = body['remark']
                dealid = body['dealid']
                salecode = ""
                salecodeid = ""
                opendate = ""
                memberid = ""
                if 'salecode' in body:
                    salecode = body['salecode']
                if 'memberid' in body:
                    memberid = body['memberid']
                if 'opendate' in body:
                    opendate = body['opendate']
                tokenreturn=checkAToken(userid,appid,atoken)
                if tokenreturn == "0":
                    userres = userorgTable.scan(
                        FilterExpression=Attr('domainid').eq(domainid) & Attr('orgid').eq(
                            orgid) & Attr('t1').eq('300') & Attr('userid').eq(userid)
                    )
                    if len(userres['Items']) == 0:
                        if memberid != "":
                            userid = memberid
                    # cursor=con.cursor()
                        sql="SELECT * FROM crmdeal WHERE dealid=%s AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
                        data=(dealid,domainid,userid,appid,orgid)
                        cursor.execute(sql,data)
                        deallist = cursor.fetchall()
                    else:
                        sql="SELECT * FROM crmdeal WHERE dealid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                        data=(dealid,domainid,appid,orgid)
                        cursor.execute(sql,data)
                        deallist = cursor.fetchall()
                    if len(deallist) == 0: 
                        response = {
                            'returncode': "200" ,
                            'status': "Invalid Deal!"
                        }
                        return cb(200,response)
                    else:
                        dateFormat = "%Y-%m-%d"
                        now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                        if closingdate == "":
                            closingdate = targetdate
                        if opendate == "":
                            opendate = now_asia.strftime(dateFormat)
                        if salecode != "":
                            selecodesql="SELECT * FROM crmsalecode WHERE salecode=%s"
                            salecodedata=(salecode,)
                            cursor.execute(selecodesql,salecodedata)
                            salecodelist = cursor.fetchall()
                            if len(salecodelist) == 0:
                                localFormat = "%Y%m%d%H%M%S"
                                timestamp =str(int(time.time()*1000.0))
                                salecodeid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))
                                sortby = now_asia.strftime(localFormat)
                                salesql = "INSERT INTO crmsalecode(salecodeid,domainid,appid,orgid,userid,salecode,sortby) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                                saledata = (salecodeid,domainid,appid,orgid,userid,salecode,sortby)
                                cursor.execute(salesql, saledata)
                            else:
                                salecodeid = salecodelist[0][1]
                        updsql = "UPDATE crmdeal SET customerid=%s, targetdate=%s,closingdate=%s,currency=%s, amount=%s, product=%s ,status=%s , stageid=%s, remark=%s,salecode=%s,opendate=%s WHERE dealid=%s AND domainid=%s"
                        upddata = (customerid,targetdate,closingdate,currency,amount,str(product),status,stageid,remark,salecodeid,opendate,dealid,domainid)
                        cursor.execute(updsql, upddata)
                        if len(product) > 0:
                            for ii in range(len(product)):
                                seleprosql="SELECT * FROM crmproduct WHERE skucode=%s"
                                seleprodata=(product[ii]['skucode'],)
                                cursor.execute(seleprosql,seleprodata)
                                productdata = cursor.fetchall()
                                if len(productdata) == 0: 
                                    localFormat = "%Y%m%d%H%M%S"
                                    now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                                    timestamp =str(int(time.time()*1000.0))
                                    productid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))                  
                                    sortby = now_asia.strftime(localFormat)
                                    prosql = "INSERT INTO crmproduct(productid,domainid,appid,orgid,userid,skucode, name,price,sortby) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                    prodata = (productid,domainid,appid,orgid,userid,product[ii]['skucode'],product[ii]['name'],product[ii]['price'],sortby)
                                    cursor.execute(prosql, prodata)
                                else:
                                    if productdata[0][7] != product[ii]['name'] or productdata[0][8] != product[ii]['price']:
                                        updatesql = "UPDATE crmproduct SET name=%s,price=%s WHERE productid=%s"
                                        updatedata = (product[ii]['name'],product[ii]['price'],productdata[0][1])
                                        cursor.execute(updatesql, updatedata)
                        con.commit()
                        reObj = dict()
                        deallistreturn = []
                        stagesql="SELECT * FROM crmdealstage where stageid=%s"
                        stagedata=(stageid,) 
                        cursor.execute(stagesql,stagedata)
                        stagelist = cursor.fetchall()
                        # filterstageid = list(filter(lambda x: x[1] ==  stageid, stagelist))
                        reObj['stagename'] = stagelist[0][2]
                        sql="SELECT * FROM crmcustomer WHERE customerid=%s"
                        data=(customerid,)
                        cursor.execute(sql,data)
                        customerlist = cursor.fetchall()
                        reObj['customerid'] = customerid
                        reObj['customername'] = customerlist[0][6]
                        reObj['dealid'] = dealid
                        reObj['product'] = product
                        returntarget = ""
                        returnclosing = ""
                        if targetdate != "":
                            tyear = str(targetdate)[0:4]
                            tmonth = str(targetdate)[5:7]
                            tday = str(targetdate)[8:10]
                            returntarget = tday+"/"+tmonth+"/"+tyear
                        else:
                            returntarget = ""
                        if closingdate != "":
                            cyear = str(closingdate)[0:4]
                            cmonth = str(closingdate)[5:7]
                            cday = str(closingdate)[8:10]
                            returnclosing = cday+"/"+cmonth+"/"+cyear
                        else:
                            returnclosing = tday+"/"+tmonth+"/"+tyear
                        oyear = str(opendate)[0:4]
                        omonth = str(opendate)[5:7]
                        oday = str(opendate)[8:10]
                        returnopen = oday+"/"+omonth+"/"+oyear
                        reObj['salecode'] = salecode
                        reObj['opendate'] = returnopen
                        reObj['targetdate'] = returntarget
                        reObj['closingdate'] = returnclosing
                        reObj['currency'] = currency
                        reObj['amount'] = amount
                        reObj['status'] = status
                        reObj['stageid'] = stageid
                        reObj['remark'] = remark
                        
                        reObj_copy=reObj.copy()
                        deallistreturn.append(reObj_copy)
                        response = {
                            'returncode':"300",
                            'data' :deallistreturn,
                            'status': "Updated Successfully!"
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
