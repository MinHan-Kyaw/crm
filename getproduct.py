import json
import requests
import sys
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
                    sql="SELECT * FROM crmproduct"
                    data=() 
                    cursor.execute(sql,data)
                    productlist = cursor.fetchall()
                    if len(productlist) == 0: 
                        response = {
                            'returncode': "300" ,
                            'list': []
                        }
                        return cb(200,response)
                    else:
                        reObj=dict()
                        productlistreturn=list()
                        for i in range(len(productlist)):
                            reObj['skucode']=productlist[i][6]
                            reObj['name'] = productlist[i][7]
                            reObj['price'] = productlist[i][8]
                            reObj_copy=reObj.copy()
                            productlistreturn.append(reObj_copy)
                        forRes=dict()
                        forRes['returncode']="300"
                        forRes['list']=productlistreturn
                        forRes['total']=len(productlistreturn)
                        forRes['status']="Product List!"
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
