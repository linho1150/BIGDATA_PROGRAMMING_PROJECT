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

async def save_log(log,busnumber,ex):
    now = datetime.datetime.now()
    now_time = now.strftime("%y%m%d%H%M%S")
    file1 = open("ERROR_LOG.txt", "a")
    file1.write("[" + str(now_time) + "]" +log+"\n"+ str(busnumber) + "정보를 DATABASE에 데이터를 저장하지 못하였습니다.\n" + str(ex) + "\n")
    file1.close()

async def main(totalbuslist,index,conn):
        try:
            for busnumber in totalbuslist:
                busnum=[busdetail(await busnumberToid(busnumber))]
                result=await asyncio.gather(*busnum)
                for bus in result:
                    for stationnm, exist in bus:
                        str_exist=str(exist).strip().replace(" ","")
                        str_stationnm=str(stationnm).strip().replace(" ","")
                        str_before_exist =  await mysql_search_data(busnumber, str_stationnm,conn)
                        if str_exist != str_before_exist or index==0:
                            await mysql_insert_data(busnumber, str_stationnm, str_exist,index,conn)
                print("confirm : ",str(index))
        except Exception as ex:
            await save_log("function main",busnumber,ex)
            print("main",ex);
            pass

async def mysql_insert_data(busnumber,stationnumber,exist,index,conn):
    try:
        #시간시작----------------------------------------------------
        str_before_time = await mysql_get_time_data(busnumber, stationnumber,conn)
        try:
            now = datetime.datetime.now()
            now_time = now.strftime("%y-%m-%d %H:%M:%S")
            wtime=str((now-str_before_time).seconds)
        except:
            wtime="0"
            pass
        # 시간끝----------------------------------------------------

        #MYSQL insert----------------------------------------------
        curs = conn.cursor()
        sql = """INSERT INTO BUS_LIVE VALUES(%s,%s,%s,%s,%s,%s)"""
        curs.execute(sql, (index, busnumber, stationnumber, now_time, exist, wtime))
        conn.commit()
        print(index," ",busnumber,",",stationnumber,"find")
        # MYSQL insert----------------------------------------------

    except Exception as ex:
        await save_log("function mysql_insert_data",busnumber,ex)
        print("mysql_insert_data ", ex);
        pass

async def mysql_get_time_data(busnumber,stationnumber,conn):
    try:
        curs = conn.cursor()
        sql = """SELECT TIME FROM BUS_LIVE WHERE BUS_ID=%s AND STATION_ID=%s AND BUS_IS='Y' ORDER BY INDEX_NUM DESC LIMIT 1"""
        curs.execute(sql, (busnumber,stationnumber))
        result = curs.fetchall()
        if len(result) == 0:
            return datetime.datetime.strptime('2019-11-24 00:01:00', "%Y-%m-%d %H:%M:%S")
        for row_data in result:
            return row_data[0]
    except Exception as ex:
        await save_log("function mysql_get_time_data", busnumber, ex)
        print("mysql_get_time_data ", ex);
        pass


async def mysql_search_data(busnumber,stationnumber,conn):
    try:
        curs = conn.cursor()
        sql = """SELECT BUS_IS FROM BUS_LIVE WHERE BUS_ID=%s AND STATION_ID=%s ORDER BY INDEX_NUM DESC LIMIT 1"""
        curs.execute(sql, (busnumber,stationnumber))
        result = curs.fetchall()
        if len(result) == 0:
            return "N"
        for row_data in result:
            return row_data[0]
    except Exception as ex:
        await save_log("function mysql_search_data", busnumber, ex)
        print("mysql_search_data ", ex);
        pass

#프로세스 시작--------------------------------------
totalbuslist=buslist_file_load()
index=30000
conn = pymysql.connect(host='bus-live.canb61sq58tb.ap-northeast-2.rds.amazonaws.com', user='admin',password='wowns0034', db='bus-live', charset='utf8')
while(True):
    time.sleep(1)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(totalbuslist,index,conn))
    index=index+1
conn.close()
loop.close()

