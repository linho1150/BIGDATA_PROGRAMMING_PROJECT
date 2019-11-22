import asyncio
import aiohttp
import json
import pypyodbc
import time
import pymysql

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

async def main(totalbuslist):
        try:
            Tbusnumber = ""
            Tstationnm = ""
            Texist = ""
            for busnumber in totalbuslist:
                busnum=[busdetail(await busnumberToid(busnumber))]
                result=await asyncio.gather(*busnum)
                for bus in result:
                    for stationnm, exist in bus:
                        if exist=="Y" and Texist != exist:
                            mysql_insert_data(busnumber, stationnm.strip(), exist.strip())
                            Texist = exist
                            print(busnumber,",",stationnm.strip(),"의 변동이 확인되었습니다.")


        except Exception as ex:  # 에러 종류:
            print(busnumber,"정보를 DATABASE에 데이터를 저장하지 못하였습니다.\n",ex)
            pass

def mysql_insert_data(busnumber,stationnumber,exist):
    # MySQL초기화시작---------------------------------------
    conn = pymysql.connect(host='bus-live.canb61sq58tb.ap-northeast-2.rds.amazonaws.com', user='admin',
                           password='wowns0034', db='bus-live', charset='utf8')
    # MySQL초기화완료---------------------------------------
    curs = conn.cursor()
    sql = """INSERT INTO BUS_LIVE VALUES(%s,%s,%s,%s)"""
    now = time.localtime()
    curs.execute(sql, (busnumber, stationnumber, int(str(now.tm_year)+str(now.tm_mon)+str(now.tm_mday)+str(now.tm_hour)+str(now.tm_min)+str(now.tm_sec)), exist))
    conn.commit()
    conn.close()

#프로세스 시작--------------------------------------
totalbuslist=buslist_file_load()

while(True):
    time.sleep(1)
    asyncio.run(main(totalbuslist))


