import json
import requests
import sys
from decimal import Decimal

# import jwt


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
                con = common.connect()
                cursor=con.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS crmlead(autoid serial PRIMARY KEY, leadid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),leadtype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),product TEXT, post VARCHAR(255), organization VARCHAR(255), currency VARCHAR(20), amount VARCHAR(50),note VARCHAR(255), date VARCHAR(20), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcustomer(autoid serial PRIMARY KEY, customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),customertype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcontact(autoid serial PRIMARY KEY,contactid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),post VARCHAR(255), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmdeal(autoid serial PRIMARY KEY, dealid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), targetdate Date, closingdate Date, currency VARCHAR(20), amount VARCHAR(50), product TEXT, status varchar(20), stageid varchar(20), remark TEXT, sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),salecode VARCHAR(50),opendate Date)")
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
                    sql="SELECT * FROM crmdeal WHERE domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
                    data=(domainid,userid,appid,orgid)
                    cursor.execute(sql,data)
                    deallist = cursor.fetchall()
                    if len(deallist) == 0: 
                        response = {
                            'returncode': "300" ,
                            'list': []
                        }
                        return cb(200,response)
                    else:   
                        stagesql="SELECT * FROM crmdealstage"
                        stagedata=() 
                        cursor.execute(stagesql,stagedata)
                        stagelist = cursor.fetchall()
                        salecodesql="SELECT * FROM crmsalecode"
                        salecodedata=() 
                        cursor.execute(salecodesql,salecodedata)
                        salecodelist = cursor.fetchall()
                        sql="SELECT * FROM crmcustomer WHERE domainid=%s AND appid=%s AND orgid=%s"
                        data=(domainid,appid,orgid)
                        cursor.execute(sql,data)
                        customerlist = cursor.fetchall()
                        deallistreturn = [] 
                        for i in range(len(deallist)):
                            dealreObj = dict()     
                            dealreObj['dealid'] = deallist[i][1]
                            dealreObj['customerid'] = deallist[i][2]
                            filterstageid = list(filter(lambda x: x[1] ==  deallist[i][13], stagelist))
                            filtercustomerid = list(filter(lambda x: x[1] ==  deallist[i][2], customerlist))
                            if len(filtercustomerid) == 0:
                                dealreObj['customername'] = ""
                            else:
                                dealreObj['customername'] = filtercustomerid[0][6]
                            dealreObj['product'] = eval(deallist[i][11])
                            if deallist[i][27] != None:
                                oyear = str(deallist[i][27])[0:4]
                                omonth = str(deallist[i][27])[5:7]
                                oday = str(deallist[i][27])[8:10]
                                dealreObj['opendate'] = oday+"/"+omonth+"/"+oyear
                            else:
                                dealreObj['opendate'] = ""
                            filtersalecodeid = list(filter(lambda x: x[1] ==  deallist[i][26], salecodelist))
                            if len(filtersalecodeid) > 0:    
                                dealreObj['salecode'] =filtersalecodeid[0][6]
                            else:
                                dealreObj['salecode'] =deallist[i][26]
                            if deallist[i][7] != "":
                                tyear = str(deallist[i][7])[0:4]
                                tmonth = str(deallist[i][7])[5:7]
                                tday = str(deallist[i][7])[8:10]
                                dealreObj['targetdate'] = tday+"/"+tmonth+"/"+tyear
                            else:
                                dealreObj['targetdate'] = ""
                            if deallist[i][8] != "":
                                cyear = str(deallist[i][8])[0:4]
                                cmonth = str(deallist[i][8])[5:7]
                                cday = str(deallist[i][8])[8:10]
                                dealreObj['closingdate'] = cday+"/"+cmonth+"/"+cyear
                            else:
                                dealreObj['closingdate'] = ""
                            dealreObj['currency'] = deallist[i][9]
                            dealreObj['amount'] = deallist[i][10]
                            dealreObj['status'] = deallist[i][12]
                            dealreObj['stageid'] = deallist[i][13]
                            dealreObj['remark'] = deallist[i][14]
                            if len(filterstageid) == 0: 
                                dealreObj['stagename'] = ""
                            else:
                                dealreObj['stagename'] = filterstageid[0][2]
                            dealreObj_copy=dealreObj.copy()
                            deallistreturn.append(dealreObj_copy)
                        response = {
                            'returncode':"300",
                            'list' : deallistreturn,
                            'total': len(deallistreturn),
                            'status': "Deal List!"
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
