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
import common

s3value = common.GetBucketSecret()
ACCESS_ID = s3value['access_id']
SECRET_KEY = s3value['secret_key']

dynamodb = boto3.resource('dynamodb')
client = boto3.client('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key=SECRET_KEY)
s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key=SECRET_KEY)
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
            if "userid" not in body or "atoken" not in body or "appid" not in body or "domainid" not in body or "orgid" not in body or "customerid" not in body or "customername" not in body or "customertype" not in body or "customermobile" not in body or "customeremail" not in body or "customeraddress1" not in body or "customeraddress2" not in body or "status" not in body or "filename" not in body or "deletefilename" not in body or 'contactlist' not in body or 'deletecontactlist' not in body or 'deletedeallist' not in body or 'deallist' not in body:
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
                # cursor.execute("CREATE TABLE IF NOT EXISTS crmdeal(autoid serial PRIMARY KEY, dealid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), targetdate Date, closingdate Date, currency VARCHAR(20), amount VARCHAR(50), product TEXT, status varchar(20), stageid varchar(20), remark TEXT,sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),salecode VARCHAR(50),opendate Date)")
                # cursor.execute("CREATE TABLE IF NOT EXISTS crmproduct(autoid serial PRIMARY KEY,productid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),skucode varchar(20), name VARCHAR(255),price varchar(50), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                # cursor.execute("CREATE TABLE IF NOT EXISTS crmsalecode(autoid serial PRIMARY KEY,salecodeid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),salecode varchar(20), sortby VARCHAR(50))")
                # con.commit()      
                userid = body['userid']
                atoken = body['atoken']
                appid = body['appid']  
                domainid = body['domainid'] 
                orgid = body['orgid']
                customerid = body['customerid']
                customername = body['customername']
                customertype = body['customertype']
                customermobile = body['customermobile']
                customeremail = body['customeremail']
                customeraddress1 = body['customeraddress1']
                customeraddress2 = body['customeraddress2']
                status = body['status']
                filename = body['filename']
                deletefilename = body['deletefilename']
                # contactname = body['contactname']
                # post = body['post']
                contactlist = []
                deletecontactlist = []
                deallist = []
                deletedeallist = []
                industrytype = ""
                if 'contactlist' in body:
                    contactlist = body['contactlist']
                if 'deletecontactlist' in body:
                    deletecontactlist = body['deletecontactlist']
                if 'deallist' in body:
                        deallist = body['deallist']
                if 'deletedeallist' in body:
                    deletedeallist = body['deletedeallist']
                if 'industrytype' in body:
                    industrytype = body['industrytype']
                # contactid = body['contactid']
                # contactmobile = body['contactmobile']
                # contactemail = body['contactemail']
                # contactaddress1 = body['contactaddress1']
                # contactaddress2 = body['contactaddress2']
                # product = body['product']
                # date = body['date']
                # currency = body['currency']
                # amount = body['amount']
                # dealid = body['dealid']
                tokenreturn=checkAToken(userid,appid,atoken)
                if tokenreturn == "0":
                    bucket = s3.Bucket('kunyekbucket')
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
                        selecodesql="SELECT * FROM crmsalecode"
                        salecodedata=()
                        cursor.execute(selecodesql,salecodedata)
                        salecodelist = cursor.fetchall() 
                        if len(deletefilename) > 0:
                            for i in range(len(deletefilename)):
                                bucket.objects.filter(Prefix="crm/" + customerid+"/"+deletefilename[i]).delete()
                                # s3.Object('kunyekbucket',"crm/" + customerid+"/"+deletefilename[i]).delete()  
                        # con = common.connect()
                        # cursor=con.cursor()
                        localFormat = '%Y-%m-%d %H:%M:%S'
                        sortbylocalFormat = "%Y%m%d%H%M%S"
                        now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                        sortby = now_asia.strftime(sortbylocalFormat)
                        filedata = []
                        if len(filename) > 0:
                            for i in range(len(filename)):
                                fileurl = ""
                                expiretime = now_asia.strftime(localFormat)  
                                datetimenow = str(expiretime)                            
                                fileurl = client.generate_presigned_url(
                                    'get_object',
                                    Params={
                                        'Bucket': 'kunyekbucket',
                                        'Key': 'crm/'+customerid + '/' + filename[i]
                                    },
                                    ExpiresIn = 1800,
                                    HttpMethod='GET'
                                )
                                filedata.append({"filename":filename[i],"fileurl":fileurl,"fileexp":datetimenow})
                        sql = "UPDATE crmcustomer SET name=%s, customertype=%s, mobile=%s, email=%s, address1=%s, address2=%s, status=%s, sortby=%s, filename=%s,industrytype=%s WHERE customerid=%s AND domainid=%s AND orgid=%s"
                        data = (customername, customertype, customermobile, customeremail, customeraddress1, customeraddress2, status, sortby, str(filedata),industrytype,customerid,domainid,orgid)
                        cursor.execute(sql, data) 
                        if len(contactlist) > 0:
                            for i in range(len(contactlist)):
                                now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                                sortby = now_asia.strftime(sortbylocalFormat)
                                if contactlist[i]['contactid'] != "":
                                    consql = "UPDATE crmcontact SET name=%s, post=%s, mobile=%s, email=%s, address1=%s, address2=%s, sortby=%s WHERE contactid=%s AND domainid=%s AND orgid=%s"
                                    condata = (contactlist[i]['contactname'], contactlist[i]['contactpost'], contactlist[i]['contactmobile'], contactlist[i]['contactemail'], contactlist[i]['contactaddress1'], contactlist[i]['contactaddress2'], sortby, contactlist[i]['contactid'],domainid,orgid)
                                    cursor.execute(consql, condata) 
                                else:
                                    timestamp =str(int(time.time()*1000.0))
                                    contactid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))                  
                                    common.resetSerialNumber("crmcontact")              
                                    coninsertsql = "INSERT INTO crmcontact(contactid, customerid,domainid,appid,orgid,userid ,name , post,mobile,email,address1,address2,sortby) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                    coninsertdata = (contactid,customerid,domainid,appid,orgid,userid, contactlist[i]['contactname'],contactlist[i]['contactpost'],contactlist[i]['contactmobile'],contactlist[i]['contactemail'],contactlist[i]['contactaddress1'],contactlist[i]['contactaddress2'],sortby)
                                    cursor.execute(coninsertsql, coninsertdata) 
                        if len(deletecontactlist) > 0:
                            for i in range(len(deletecontactlist)):
                                deleteconsql="DELETE FROM crmcontact WHERE contactid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                                deletecondata=(deletecontactlist[i]['contactid'],domainid,appid,orgid)
                                cursor.execute(deleteconsql,deletecondata)
                        dateFormat = "%Y-%m-%d"
                        if len(deallist) > 0:
                                                       
                            for i in range(len(deallist)):
                                # seledealsql="SELECT * FROM crmdeal WHERE dealid=%s"
                                # seledealdata=(deallist[i]['dealid'],)
                                # cursor.execute(seledealsql,seledealdata)
                                # seledealdata = cursor.fetchall()
                                if deallist[i]['closingdate'] == "":
                                    deallist[i]['closingdate'] = deallist[i]['targetdate']
                                opendate = now_asia.strftime(dateFormat)
                                salecode = ""
                                salecodeid = ""
                                if 'salecode' in deallist[i]:
                                    salecode = deallist[i]['salecode']  
                                if salecode != "":
                                    filtersalecodeid = list(filter(lambda x: x[6] ==  salecode, salecodelist))                                    
                                    if len(filtersalecodeid) == 0:
                                        timestamp =str(int(time.time()*1000.0))
                                        salecodeid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))
                                        sortby = now_asia.strftime(sortbylocalFormat)
                                        common.resetSerialNumber("crmsalecode")              
                                        salesql = "INSERT INTO crmsalecode(salecodeid,domainid,appid,orgid,userid,salecode,sortby) VALUES(%s,%s,%s,%s,%s,%s,%s)"
                                        saledata = (salecodeid,domainid,appid,orgid,userid,salecode,sortby)
                                        cursor.execute(salesql, saledata)                              
                                    else:
                                        salecodeid =filtersalecodeid[0][1]
                                if 'opendate' in deallist[i]:
                                    opendate = deallist[i]['opendate']
                                if deallist[i]['dealid'] != "":
                                    updealsql = "UPDATE crmdeal SET customerid=%s, product=%s, targetdate=%s, closingdate=%s, currency=%s, amount=%s, status=%s,stageid=%s,remark=%s,salecode=%s,opendate=%s WHERE dealid=%s"
                                    updealdata = (customerid, str(deallist[i]['product']), deallist[i]['targetdate'], deallist[i]['closingdate'], deallist[i]['currency'], deallist[i]['amount'], deallist[i]['status'], deallist[i]['stageid'],deallist[i]['remark'],salecodeid,opendate,deallist[i]['dealid'])
                                    cursor.execute(updealsql, updealdata) 
                                else:
                                    sortby = now_asia.strftime(sortbylocalFormat) 
                                    timestamp =str(int(time.time()*1000.0))
                                    
                                    dealid = str(''.join([random.choice(string.digits+timestamp) for n in range(10)]))  
                                    common.resetSerialNumber("crmdeal")              
                                    dealsql = "INSERT INTO crmdeal(dealid,customerid,domainid,appid,orgid,userid,product, targetdate, closingdate,currency,amount,status,stageid,remark,sortby,salecode,opendate) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                    dealdata = (dealid,customerid,domainid,appid,orgid,userid,str(deallist[i]['product']),deallist[i]['targetdate'],deallist[i]['closingdate'],deallist[i]['currency'],deallist[i]['amount'],deallist[i]['status'],deallist[i]['stageid'],deallist[i]['remark'],sortby,salecode,opendate)
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
                        if len(deletedeallist) > 0:
                            for i in range(len(deletedeallist)):
                                deletedelsql="DELETE FROM crmdeal WHERE dealid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                                deletedeldata=(deletedeallist[i]['dealid'],domainid,appid,orgid)
                                cursor.execute(deletedelsql,deletedeldata)
               
                        con.commit()
                        consql="SELECT * FROM crmcontact WHERE customerid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                        condata=(customerid,domainid,appid,orgid)
                        cursor.execute(consql,condata)
                        contactlistdata =cursor.fetchall()
                        contactreObj = dict()
                        contactlistreturn = [] 
                        if len(contactlistdata) > 0:
                            for c in range(len(contactlistdata)):
                                contactreObj['contactname'] = contactlistdata[c][7]
                                contactreObj['contactid'] = contactlistdata[c][1]
                                contactreObj['contactpost'] = contactlistdata[c][8]
                                contactreObj['contactmobile'] = contactlistdata[c][9]
                                contactreObj['contactemail'] = contactlistdata[c][10]
                                contactreObj['contactaddress1'] = contactlistdata[c][11]
                                contactreObj['contactaddress2'] = contactlistdata[c][12]
                                contactreObj_copy=contactreObj.copy()
                                contactlistreturn.append(contactreObj_copy)
                        dealsql="SELECT * FROM crmdeal WHERE customerid=%s AND domainid=%s AND appid=%s AND orgid=%s"
                        dealdata=(customerid,domainid,appid,orgid)
                        cursor.execute(dealsql,dealdata)
                        deallistdata =cursor.fetchall()
                        selecodesql="SELECT * FROM crmsalecode"
                        salecodedata=()
                        cursor.execute(selecodesql,salecodedata)
                        salecodelist = cursor.fetchall() 
                        dealreObj = dict()
                        deallistreturn = [] 
                        if len(deallistdata) > 0:
                            for c in range(len(deallistdata)):
                                dealreObj['dealid'] = deallistdata[c][1]
                                if deallistdata[c][7] != "":
                                    tyear = str(deallistdata[c][7])[0:4]
                                    tmonth = str(deallistdata[c][7])[5:7]
                                    tday = str(deallistdata[c][7])[8:10]
                                    dealreObj['targetdate'] = tday+"/"+tmonth+"/"+tyear
                                else:
                                    dealreObj['targetdate'] = ""
                                if deallistdata[c][8] != "":
                                    cyear = str(deallistdata[c][8])[0:4]
                                    cmonth = str(deallistdata[c][8])[5:7]
                                    cday = str(deallistdata[c][8])[8:10]
                                    dealreObj['closingdate'] = cday+"/"+cmonth+"/"+cyear
                                else:
                                    dealreObj['closingdate'] = tday+"/"+tmonth+"/"+tyear
                                filtersalecodeid = list(filter(lambda x: x[1] ==  deallistdata[c][26], salecodelist))
                                if len(filtersalecodeid) > 0:    
                                    dealreObj['salecode'] =filtersalecodeid[0][6]
                                else:
                                    # return cb(200,deallistdata[c][26])
                                    dealreObj['salecode'] = deallistdata[c][26]
                                if deallistdata[c][27] != "":
                                    oyear = str(deallistdata[c][27])[0:4]
                                    omonth = str(deallistdata[c][27])[5:7]
                                    oday = str(deallistdata[c][27])[8:10]
                                    dealreObj['opendate'] = oday+"/"+omonth+"/"+oyear
                                else:
                                    dealreObj['opendate'] = ""
                                dealreObj['currency'] = deallistdata[c][9]
                                dealreObj['amount'] = deallistdata[c][10]
                                dealreObj['status'] = deallistdata[c][12]
                                dealreObj['stageid'] = deallistdata[c][13]
                                dealreObj['remark'] = deallistdata[c][14]
                                dealreObj['product'] = eval(deallistdata[c][11])
                                dealreObj_copy=dealreObj.copy()
                                deallistreturn.append(dealreObj_copy)
                        reObj=dict()
                        customerlistreturn=list()  
                        # contactreObj['contactname'] = contactname
                        # contactreObj['contactid'] = contactid
                        # contactreObj['contactpost'] = post
                        # contactreObj['contactmobile'] = contactmobile
                        # contactreObj['contactemail'] = contactemail
                        # contactreObj['contactaddress1'] = contactaddress1
                        # contactreObj['contactaddress2'] = contactaddress2
                        # contactreObj_copy=contactreObj.copy()
                        # contactlistreturn.append(contactreObj_copy)
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
                        reObj['industrytype'] = industrytype
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
