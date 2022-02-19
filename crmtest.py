
import json
import requests
import boto3
import sys
# from backports.zoneinfo import ZoneInfo
from datetime import date, datetime, time
from datetime import datetime
from datetime import timedelta
from boto3.dynamodb.conditions import Key, Attr
from backports.zoneinfo import ZoneInfo
from decimal import Decimal
# import jwt
import common

s3value = common.GetBucketSecret()
ACCESS_ID = s3value['access_id']
SECRET_KEY = s3value['secret_key']
dynamodb = boto3.resource('dynamodb')
userorgtable = dynamodb.Table('UserOrganizations')
hierarchyTable = dynamodb.Table('Hierarchy')
userTable = dynamodb.Table('kunyekusers')
domainsTable = dynamodb.Table('Domains')
client = boto3.client('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key=SECRET_KEY)


headers = {
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
}


def lambda_handler(event, context):
    if event['httpMethod'] == "POST":
        try:
            body = json.loads(event['body'])
            # header=event['headers']
            # btoken=header['btoken']
            # puserid=header['puserid']
            # pappid=header['pappid']
            if "userid" not in body or "atoken" not in body or "appid" not in body or "domainid" not in body or 'orgid' not in body or 'startdate' not in body or 'enddate' not in body or 'status' not in body or 'stageid' not in body:
                body = {
                    'returncode': "200",
                    'status': "Missing Field!"
                }
                return cb(200, body)
            else:
                con = common.connect()
                cursor = con.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS crmlead(autoid serial PRIMARY KEY, leadid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),leadtype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),product TEXT, post VARCHAR(255), organization VARCHAR(255), currency VARCHAR(20), amount VARCHAR(50),note VARCHAR(255), date VARCHAR(20), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcustomer(autoid serial PRIMARY KEY, customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),customertype VARCHAR(20), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255), status VARCHAR(10),sortby VARCHAR(50),filename TEXT, t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmcontact(autoid serial PRIMARY KEY,contactid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), name VARCHAR(255),post VARCHAR(255), mobile VARCHAR(255), email VARCHAR(255), address1 VARCHAR(255),address2 VARCHAR(255),sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmdeal(autoid serial PRIMARY KEY, dealid VARCHAR(10), customerid VARCHAR(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50), targetdate Date, closingdate Date, currency VARCHAR(20), amount VARCHAR(50), product TEXT, status varchar(20), stageid varchar(20), remark TEXT, sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255),salecode VARCHAR(50),opendate Date)")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmproduct(autoid serial PRIMARY KEY,productid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),skucode varchar(20), name VARCHAR(255), sortby VARCHAR(50),t1 VARCHAR(255),t2 VARCHAR(255),t3 VARCHAR(255),t4 VARCHAR(255),t5 VARCHAR(255),t6 VARCHAR(255),t7 VARCHAR(255),t8 VARCHAR(255),t9 VARCHAR(255),t10 VARCHAR(255))")
                cursor.execute("CREATE TABLE IF NOT EXISTS crmsalecode(autoid serial PRIMARY KEY,salecodeid varchar(10),domainid varchar(20),appid varchar(20),orgid varchar(20), userid varchar(50),salecode varchar(50), sortby VARCHAR(50))")
                con.commit()
                userid = body['userid']
                atoken = body['atoken']
                appid = body['appid']
                domainid = body['domainid']
                orgid = body['orgid']
                startdate = body['startdate']
                enddate = body['enddate']
                status = body['status']
                stageid = body['stageid']
                salecode = ""
                memberid = ""
                if 'memberid' in body:
                    memberid = body['memberid']
                if 'salecode' in body:
                    salecode = body['salecode']
                # btoken=_json['btoken']
                # puserid=_json['puserid']
                # pappid=_json['pappid']
                tokenreturn = checkAToken(userid, appid, atoken)
                if tokenreturn == "0":
                    domaindatares = domainsTable.scan(
                        FilterExpression=Attr('domainid').eq(domainid)
                    )
                    domaindatalist = domaindatares['Items']
                    user = userTable.scan(
                        FilterExpression=Attr('domain').eq(
                            domaindatalist[0]['shortcode'])
                    )
                    userData = user['Items']
                    l = list()
                    if memberid == "":
                        orguserRes = userorgtable.scan(
                            FilterExpression=Attr('orgid').eq(
                                orgid) & Attr('domainid').eq(domainid)
                        )
                        orguserData = orguserRes['Items']
                        
                        # l = list()
                        filteruserData = list(
                            filter(lambda x: x['userid'] == userid, orguserData))

                        if len(filteruserData) > 0:

                            hierarchyRes = hierarchyTable.scan(
                                FilterExpression=Attr('orgid').eq(orgid) & Attr(
                                    'domainid').eq(domainid) & Attr('type').eq("1")
                            )
                            hierarchyData = hierarchyRes['Items']
                            
                            # ceid = filteruserData[0]['employeeid']
                            filteruserposData = list(
                                filter(lambda x: x['peid'] == filteruserData[0]['employeeid'], hierarchyData))
                            # if len(filteruserposData)>0:
                            #     for i in range(len(filteruserposData)):
                            #         filteruserposData[i]['pid']=""
                            #         filteruserposData[i]['peid']=""
                            # return cb(200,filteruserposData)
                            tchild = list()
                            finalHierarchy = list()
                            tchild = filteruserposData
                            finalHierarchy = filteruserposData
                            while len(tchild) > 0:
                                temList = list()
                                for hierarchydata in tchild:
                                    filterhierarchyData = list(filter(
                                        lambda x: x['pid'] == hierarchydata['cid'] and x['peid'] == hierarchydata['ceid'], hierarchyData))
                                    temList += filterhierarchyData
                                tchild = temList

                                finalHierarchy += temList
                            data = finalHierarchy
                            
                            # get hierarchy user's name and position name
                            for i in range(len(data)):
                                d = dict()
                                useridData = list(
                                    filter(lambda x: x['employeeid'] == data[i]['ceid'], orguserData))

                                if len(useridData) > 0:
                                    d['userid'] = useridData[0]['userid']
                                    d['username'] = useridData[0]['username']
                                    url = ""
                                    filterhieruserid = list(
                                        filter(lambda x: x['userid'] == useridData[0]['userid'], userData))
                                    if len(filterhieruserid) > 0:
                                        if filterhieruserid[0]['imagename'] != "":
                                            if filterhieruserid[0]['n1'] != "":
                                                localFormat = '%Y-%m-%d %H:%M:%S'
                                                now_asia = datetime.now(
                                                    ZoneInfo("Asia/Yangon"))
                                                expiretime = now_asia.strftime(
                                                    localFormat)
                                                datetimedata = filterhieruserid[0]['n1']
                                                datetimenow = str(expiretime)
                                                expire = datetime.strptime(
                                                    datetimedata, localFormat)
                                                expire2 = datetime.strptime(
                                                    datetimenow, localFormat)
                                                minutes_diff = (
                                                    expire2 - expire).total_seconds() / 60.0
                                                if int(minutes_diff) >= 30:
                                                    url = client.generate_presigned_url(
                                                        'get_object',
                                                        Params={
                                                            'Bucket': 'kunyekbucket',
                                                            'Key': 'user' + '/' + filterhieruserid[0]['imagename']
                                                        },
                                                        ExpiresIn=1800,
                                                        HttpMethod='GET'
                                                    )
                                                else:
                                                    url = filterhieruserid[0]['t1']
                                    d['imagename'] = url

                                    duplicateRes = list(
                                        filter(lambda x: x['userid'] == d['userid'], l))
                                    if len(duplicateRes) == 0:
                                        l.append(d.copy())
                    else:
                        userid = memberid
                    # return cb(200,l)
                    # filteruserData = list(
                    #     filter(lambda x: x['userid'] == searchuserid, orguserData))
                    salecodesql = "SELECT * FROM crmsalecode"
                    salecodedata = ()
                    cursor.execute(salecodesql, salecodedata)
                    salecodelist = cursor.fetchall()
                    if len(l) == 0:
                        u = dict()
                        u['userid'] = userid
                        url = ""
                        u['username'] = ""
                        filterhieruserid = list(
                            filter(lambda x: x['userid'] == userid, userData))
                        if len(filterhieruserid) > 0:
                            u['username'] = filterhieruserid[0]['username']
                            if filterhieruserid[0]['imagename'] != "":
                                if filterhieruserid[0]['n1'] != "":
                                    localFormat = '%Y-%m-%d %H:%M:%S'
                                    now_asia = datetime.now(
                                        ZoneInfo("Asia/Yangon"))
                                    expiretime = now_asia.strftime(
                                        localFormat)
                                    datetimedata = filterhieruserid[0]['n1']
                                    datetimenow = str(expiretime)
                                    expire = datetime.strptime(
                                        datetimedata, localFormat)
                                    expire2 = datetime.strptime(
                                        datetimenow, localFormat)
                                    minutes_diff = (
                                        expire2 - expire).total_seconds() / 60.0
                                    if int(minutes_diff) >= 30:
                                        url = client.generate_presigned_url(
                                            'get_object',
                                            Params={
                                                'Bucket': 'kunyekbucket',
                                                'Key': 'user' + '/' + filterhieruserid[0]['imagename']
                                            },
                                            ExpiresIn=1800,
                                            HttpMethod='GET'
                                        )
                                    else:
                                        url = filterhieruserid[0]['t1']
                        u['imagename'] = url
                        l.append(u.copy())
                    returndata = getdata(status, stageid, salecode, salecodelist, startdate,
                            enddate, domainid, appid, orgid, cursor, l)
                    # return cb(200,returndata)
                    filtermemberlist = list(filter(lambda x: x['userid'] != userid, l))
                    response = {
                        'returncode':"300",
                        'list' : returndata['deallist'],
                        'total': returndata['totalprice'],
                        'memberlist': filtermemberlist,
                        'status': "Success!"
                    }
                    return cb(200,response)
                    
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
    return "0"


def getdata(status, stageid, salecode, salecodelist, startdate, enddate, domainid,appid, orgid, cursor, userlist):
    deallistreturn = []
    totalpricereturn = []
    stagesql = "SELECT * FROM crmdealstage"
    stagedata = ()
    cursor.execute(stagesql, stagedata)
    stagelist = cursor.fetchall()
    for u in range(len(userlist)):
        deallist = []
        if status != "" and stageid != "":
            if salecode != "":
                filtersalecodeid = list(
                    filter(lambda x: x[6] == salecode, salecodelist))
                if len(filtersalecodeid) > 0:
                    salecode = filtersalecodeid[0][1]
                sql = "SELECT * FROM crmdeal WHERE (targetdate BETWEEN %s AND %s) AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s AND status=%s AND stageid=%s AND salecode=%s"
                data = (startdate, enddate, domainid, userlist[u]['userid'],
                        appid, orgid, status, stageid, salecode)
            else:
                sql = "SELECT * FROM crmdeal WHERE (targetdate BETWEEN %s AND %s) AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s AND status=%s AND stageid=%s"
                data = (startdate, enddate, domainid, userlist[u]['userid'],
                        appid, orgid, status, stageid)
        if status != "" and stageid == "":
            if salecode != "":
                filtersalecodeid = list(
                    filter(lambda x: x[6] == salecode, salecodelist))
                if len(filtersalecodeid) > 0:
                    salecode = filtersalecodeid[0][1]
                sql = "SELECT * FROM crmdeal WHERE (targetdate BETWEEN %s AND %s) AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s AND status=%s AND salecode=%s"
                data = (startdate, enddate, domainid, userlist[u]['userid'],
                        appid, orgid, status, salecode)
            else:
                sql = "SELECT * FROM crmdeal WHERE (targetdate BETWEEN %s AND %s) AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s AND status=%s"
                data = (startdate, enddate, domainid,
                        userlist[u]['userid'], appid, orgid, status)
        if status == "" and stageid != "":
            if salecode != "":
                filtersalecodeid = list(
                    filter(lambda x: x[6] == salecode, salecodelist))
                if len(filtersalecodeid) > 0:
                    salecode = filtersalecodeid[0][1]
                sql = "SELECT * FROM crmdeal WHERE (targetdate BETWEEN %s AND %s) AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s AND stageid=%s AND salecode=%s"
                data = (startdate, enddate, domainid, userlist[u]['userid'],
                        appid, orgid, stageid, salecode)
            else:
                sql = "SELECT * FROM crmdeal WHERE (targetdate BETWEEN %s AND %s) AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s AND stageid=%s"
                data = (startdate, enddate, domainid,
                        userlist[u]['userid'], appid, orgid, stageid)
        if status == "" and stageid == "":
            if salecode != "":
                filtersalecodeid = list(
                    filter(lambda x: x[6] == salecode, salecodelist))
                if len(filtersalecodeid) > 0:
                    salecode = filtersalecodeid[0][1]
                sql = "SELECT * FROM crmdeal WHERE (targetdate BETWEEN %s AND %s) AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s AND salecode=%s"
                data = (startdate, enddate, domainid,
                        userlist[u]['userid'], appid, orgid, salecode)
            else:
                sql = "SELECT * FROM crmdeal WHERE (targetdate BETWEEN %s AND %s) AND domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
                data = (startdate, enddate, domainid,
                        userlist[u]['userid'], appid, orgid)
        cursor.execute(sql, data)
        deallist = cursor.fetchall()
        if len(deallist) > 0:

            sql = "SELECT * FROM crmcustomer WHERE domainid=%s AND userid=%s AND appid=%s AND orgid=%s"
            data = (domainid, userlist[u]['userid'], appid, orgid)
            cursor.execute(sql, data)
            customerlist = cursor.fetchall()

            for i in range(len(deallist)):
                # duplicateRes = list(
                #     filter(lambda x: x['dealid'] == deallist[i][1], deallistreturn))
                # if len(duplicateRes) == 0:
                    # l.append(d.copy())
                    dealreObj = dict()
                    priceObj = dict()
                    if len(totalpricereturn) == 0:
                        if deallist[i][9] != "" and deallist[i][10] != "" and deallist[i][10] != "0":
                            priceObj['currency'] = deallist[i][9]
                            priceObj['amount'] = float(deallist[i][10])
                            priceObj_copy = priceObj.copy()
                            totalpricereturn.append(priceObj_copy)
                    else:
                        if deallist[i][9] != "" and deallist[i][10] != "" and deallist[i][10] != "0":
                            have = "false"
                            for ii in range(len(totalpricereturn)):
                                if totalpricereturn[ii]['currency'] == deallist[i][9]:
                                    totalpricereturn[ii]['amount'] = float(
                                        totalpricereturn[ii]['amount']) + float(deallist[i][10])
                                    have = "true"
                                    break
                            if have == "false":
                                priceObj['currency'] = deallist[i][9]
                                priceObj['amount'] = float(deallist[i][10])
                                priceObj_copy = priceObj.copy()
                                totalpricereturn.append(priceObj_copy)
                    dealreObj['dealid'] = deallist[i][1]
                    dealreObj['customerid'] = deallist[i][2]
                    filterstageid = list(
                        filter(lambda x: x[1] == deallist[i][13], stagelist))
                    filtercustomerid = list(
                        filter(lambda x: x[1] == deallist[i][2], customerlist))
                    if len(filtercustomerid) == 0:
                        dealreObj['customername'] = ""
                    else:
                        dealreObj['customername'] = filtercustomerid[0][6]
                    dealreObj['product'] = eval(deallist[i][11])
                    # dealreObj['targetdate'] = str(deallist[i][7])
                    # 2021-11-17
                    # 15/11/2021
                    filtersalecodeid = list(
                        filter(lambda x: x[1] == deallist[i][26], salecodelist))
                    if len(filtersalecodeid) > 0:
                        if filtersalecodeid[0][6] == None:
                            dealreObj['salecode'] = ""
                        else:    
                            dealreObj['salecode'] = filtersalecodeid[0][6]
                    else:
                        if deallist[i][26] == None:
                            dealreObj['salecode'] = ""
                        else:                        
                            dealreObj['salecode'] = deallist[i][26]
                    if deallist[i][27] != None:
                        oyear = str(deallist[i][27])[0:4]
                        omonth = str(deallist[i][27])[5:7]
                        oday = str(deallist[i][27])[8:10]
                        dealreObj['opendate'] = oday+"/"+omonth+"/"+oyear
                    else:
                        dealreObj['opendate'] = ""
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
                    dealreObj['sort'] = deallist[i][15]
                    dealreObj['userid'] = userlist[u]['userid']
                    dealreObj['username'] = userlist[u]['username']
                    dealreObj['imagename'] = userlist[u]['imagename']
                    if len(filterstageid) == 0:
                        dealreObj['stagename'] = ""
                    else:
                        dealreObj['stagename'] = filterstageid[0][2]
                    dealreObj_copy = dealreObj.copy()
                    deallistreturn.append(dealreObj_copy)
    return {"totalprice": totalpricereturn, "deallist": deallistreturn}


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
