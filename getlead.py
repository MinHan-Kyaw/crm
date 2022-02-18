import json
import requests
import sys
import boto3
# from backports.zoneinfo import ZoneInfo
from datetime import date, datetime, time
from datetime import datetime
from backports.zoneinfo import ZoneInfo
from decimal import Decimal
import psycopg2

client = boto3.client('s3')

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
            if "userid" not in body or "atoken" not in body or "appid" not in body or "orgid" not in body or "domainid" not in body:
                body={
                    'returncode':"200",
                    'status':"Missing Field!"
                }
                return cb(200,body)
            else:
                con=connect()
                cursor=con.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS crmlead(autoid serial PRIMARY KEY, leadid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),leadtype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),product TEXT, post VARCHAR(255), organization VARCHAR(255), currency VARCHAR(20), amount VARCHAR(50),note VARCHAR(255), date VARCHAR(20), status VARCHAR(10),sortby VARCHAR(50),filename VARCHAR(50), t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),industrytype TEXT,leadsource TEXT)")
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
                    sql="SELECT * FROM crmlead WHERE domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
                    data=(domainid,userid,appid,orgid) 
                    cursor.execute(sql,data)
                    leadlist = cursor.fetchall()
                    if len(leadlist) == 0: 
                        response = {
                            'returncode': "300" ,
                            'list': []
                        }
                        return cb(200,response)
                    else:
                        reObj=dict()
                        leadlistreturn=list()
                        for i in range(len(leadlist)):
                            reObj['leadid']=leadlist[i][1]
                            # reObj['domainid']= leadlist[i][2]
                            # reObj['domain']= leadlist[i][3]
                            # reObj['appid']= leadlist[i][3]
                            reObj['orgid']= leadlist[i][4]
                            reObj['userid']= leadlist[i][5]
                            reObj['name']= leadlist[i][6]
                            reObj['leadtype']= leadlist[i][7]
                            reObj['mobile'] = leadlist[i][8]
                            reObj['email']= leadlist[i][9]
                            reObj['address1']= leadlist[i][10]
                            reObj['address2']= leadlist[i][11]
                            reObj['product']= eval(leadlist[i][12])
                            reObj['post']= leadlist[i][13]
                            reObj['organization']= leadlist[i][14]
                            reObj['currency']= leadlist[i][15]
                            reObj['amount']= leadlist[i][16]
                            reObj['note']= leadlist[i][17]
                            if str(leadlist[i][18])[4:5] == "-":
                                tyear = str(leadlist[i][18])[0:4]
                                tmonth = str(leadlist[i][18])[5:7]
                                tday = str(leadlist[i][18])[8:10]
                                reObj['date'] = tday+"/"+tmonth+"/"+tyear
                            else:
                                reObj['date']= leadlist[i][18]
                            reObj['status']= leadlist[i][19]
                            reObj['sort'] = leadlist[i][20]
                            filedata = []
                            if leadlist[i][21] != "":
                                toupdate = "false"
                                leadlistdata = eval(leadlist[i][21])
                                for f in range(len(leadlistdata)):
                                    fileurl = ""
                                    localFormat = '%Y-%m-%d %H:%M:%S'
                                    now_asia = datetime.now(ZoneInfo("Asia/Yangon"))
                                    expiretime = now_asia.strftime(localFormat)  
                                    datetimenow = str(expiretime)
                                    expire2 = datetime.strptime(datetimenow, localFormat)
                                    datetimedata = leadlistdata[f]['fileexp']
                                    expire = datetime.strptime(datetimedata, localFormat)
                                    # expire2 = datetime.strptime(datetimenow, localFormat)
                                    minutes_diff = (expire2 - expire).total_seconds() / 60.0
                                    if int(minutes_diff) >= 30:
                                        toupdate = "true"
                                        fileurl = client.generate_presigned_url(
                                                'get_object',
                                                Params={
                                                    'Bucket': 'kunyekbucket',
                                                    'Key': 'crm/'+leadlist[i][1] + '/' + leadlistdata[f]['filename']
                                                },
                                                ExpiresIn = 1800,
                                                HttpMethod='GET'
                                            )
                                        filedata.append({"filename":leadlistdata[f]['filename'],"fileurl":fileurl,"fileexp":datetimenow})               
                                    else:
                                        filedata.append(leadlistdata[f])
                                if toupdate == "true":
                                    leadsql = "UPDATE crmlead SET filename=%s WHERE leadid=%s AND domainid=%s AND userid=%s"
                                    leaddata = (str(filedata), leadlist[i][1], domainid,userid)
                                    cursor.execute(leadsql, leaddata)
                                    con.commit()
                            reObj['filelist'] = filedata
                            if leadlist[i][32] == None:
                                reObj['industrytype'] = ""
                            else:
                                reObj['industrytype'] = leadlist[i][32]
                            if leadlist[i][33] == None:
                                reObj['leadsource'] = ""
                            else:
                                reObj['leadsource'] = leadlist[i][33]
                            reObj['filename'] = ""
                            reObj['fileurl'] = ""
                            reObj_copy=reObj.copy()
                            leadlistreturn.append(reObj_copy)
                        con.commit()
                        forRes=dict()
                        forRes['returncode']="300"
                        forRes['list']=leadlistreturn
                        forRes['total']=len(leadlistreturn)
                        forRes['status']="Lead List!"
                        response=forRes
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
