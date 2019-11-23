#!/usr/bin/env python3

import asyncio
import aiohttp
import json
import time
import pymysql
import datetime

def buslist_file_load():
    f = open("buslist.txt", 'rt',encoding='UTF8')
    line = f.read().splitlines()
    f.close()
    return line

async def busnumberToid(busnum):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get('http://m.bus.go.kr/mBus/bus/getBusRouteList.bms?strSrch='+busnum) as resp:
                jsondata=json.loads(await resp.text())
                return jsondata['resultList'][0]['busRouteId']
    except:
        print(busnum+"의 버스번호가 없는것 같거나, 오류가 발생했습니다.")
        pass

async def busdetail(busRouteId):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get('http://m.bus.go.kr/mBus/bus/getRouteAndPos.bms?busRouteId='+busRouteId) as resp:
                jsondata=json.loads(await resp.text())
                TEMP=[(i['stationNo'],i['existYn']) for i in jsondata['resultList']] #stationNo-정류소번호 #stationNm=정류소이름
                return TEMP
                # existYn 정류소에 버스 여부
    except:
        print(busRouteId+"의 데이터를 로드하지 못하였습니다.")
        pass

async def main(totalbuslist,index):
        try:
            for busnumber in totalbuslist:
                busnum=[busdetail(await busnumberToid(busnumber))]
                result=await asyncio.gather(*busnum)
                for bus in result:
                    for stationnm, exist in bus:
                        str_exist=str(exist).strip().replace(" ","")
                        str_stationnm=str(stationnm).strip().replace(" ","")
                        str_before_exist,str_before_time = await mysql_search_data(busnumber, str_stationnm)
                        if str_exist != str_before_exist or index==0:
                            await mysql_insert_data(busnumber, str_stationnm, str_exist,index,str_before_time)

        except Exception as ex:
            try:
                now = datetime.datetime.now()
                now_time = now.strftime("%y%m%d%H%M%S")
                print("[" + str(now_time) + "]" + str(busnumber) + "정보를 DATABASE에 데이터를 저장하지 못하였습니다.\n" + str(ex) + "\n")
                file1 = open("ERROR_LOG.txt","w")
                file1.write("["+str(now_time)+"]"+str(busnumber)+"정보를 DATABASE에 데이터를 저장하지 못하였습니다.\n"+str(ex)+"\n")
                file1.close()
            except Exception as e:
                print("ERROR",e)
                print(busnumber,"정보를 DATABASE에 데이터를 저장하지 못하였습니다.\n",ex)
                pass

async def mysql_insert_data(busnumber,stationnumber,exist,index,str_before_time):
    try:
        conn = pymysql.connect(host='bus-live.canb61sq58tb.ap-northeast-2.rds.amazonaws.com', user='admin',
                               password='wowns0034', db='bus-live', charset='utf8')
        curs = conn.cursor()
        sql = """INSERT INTO BUS_LIVE VALUES(%s,%s,%s,%s,%s,%s)"""
        now = datetime.datetime.now()
        now_time = now.strftime("%y%m%d%H%M%S")
        wtime=str(int(now_time)-int(str_before_time))
        curs.execute(sql, (index, busnumber, stationnumber, now_time, exist, wtime))
        conn.commit()
        print("[",now_time,"] ",index," ",busnumber,",",stationnumber,"의 변동이 확인되었습니다.")
        conn.close()
    except Exception as ex:
        print("MYSQL 오류",ex)
        pass


async def mysql_search_data(busnumber,stationnumber):
    try:
        conn = pymysql.connect(host='bus-live.canb61sq58tb.ap-northeast-2.rds.amazonaws.com', user='admin',
                               password='wowns0034', db='bus-live', charset='utf8')
        curs = conn.cursor() #WHERE BUS_ID=%s AND STATION_ID=%s AND INDEX_NUM=%s
        sql = """SELECT B.BUS_IS,B.TIME FROM (SELECT MAX(INDEX_NUM) AS MAX_NUM FROM BUS_LIVE WHERE BUS_ID=%s AND STATION_ID=%s) AS A, BUS_LIVE AS B WHERE BUS_ID=%s AND STATION_ID=%s AND B.INDEX_NUM=A.MAX_NUM"""
        curs.execute(sql, (busnumber,stationnumber,busnumber,stationnumber))
        result = curs.fetchall()
        if len(result) == 0:
            return "", 0
        for row_data in result:
            return row_data[0],row_data[1]
        conn.close()
    except Exception as e:
        print("MYSQL 오류2",e)

#프로세스 시작--------------------------------------
totalbuslist=buslist_file_load()
index=0
while(True):
    time.sleep(1)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(totalbuslist,index))
    index=index+1
loop.close()



