import asyncio
import aiohttp
import json
import pypyodbc

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
                TEMP=[(i['stationNm'],i['existYn']) for i in jsondata['resultList']] #stationNo-정류소번호 #stationNm=정류소이름
                return TEMP
                # existYn 정류소에 버스 여부
    except:
        print(busRouteId+"의 데이터를 로드하지 못하였습니다.")
        pass

async def main(totalbuslist):
        try:
            for busnumber in totalbuslist:
                busnum=[busdetail(await busnumberToid(busnumber))]
                result=await asyncio.gather(*busnum)
                for bus in result:
                    for stationnm, exist in bus:
                        access_insert_data(busnumber,stationnm.strip(),exist.strip())
        except:
            print(busnumber,"정보를 DATABASE에 데이터를 저장하지 못하였습니다.")
            pass

def access_insert_data(busnumber,stationnumber,exist):
    cur = dbconn.cursor()
    cur.execute("""INSERT INTO BUS_LIVE VALUES('"""+busnumber+"""','"""+stationnumber+"""',now(),'"""+exist+"""')""")
    dbconn.commit()

#DB초기화 ----------------------------------------
dbname = r'C:\Users\LINHO\BIG_DATA_PROJECT\DATA.mdb'
dir = r'C:\Users\LINHO\BIG_DATA_PROJECT'

connStr = (r"DRIVER={Microsoft Access Driver (*.mdb)};UID=admin;UserCommitSync=Yes;Threads=3;SafeTransactions=0;PageTimeout=5;axScanRows=8;MaxBufferSize=2048;FIL={MS Access};DriverId=25;DefaultDir=" + dir + ";DBQ=" + dbname)
dbconn = pypyodbc.connect(connStr)
#DB초기화 완료-------------------------------------

#프로세스 시작--------------------------------------
totalbuslist=buslist_file_load()
asyncio.run(main(totalbuslist))


