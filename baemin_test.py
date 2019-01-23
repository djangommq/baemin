# -*- coding: utf-8 -*-
import csv
import datetime
import os
import requests

"""
baemin测试脚本:  以釜山为测试地区
"""

# 数据统计列表
data_statistics=[]

date = datetime.datetime.now().strftime("%Y-%m-%d")
file_path = os.path.dirname(os.path.split(os.path.realpath(__file__))[0]) + '/crawlerOutput/' + date + '/'
print(file_path)
web_name = 'baemin/'
logFile = file_path + 'log/' + web_name + 'log.csv'
rawHead = [
    'rundate',
    'type',
    'Shop_Nm',
    'Addr',
    'Biz_No',
    'Close_Day',
    'Ct_Cd',
    'Ct_Cd_Nm',
    'Ct_Cd_Nm_En',
    'Dlvry_Info',
    'Dlvry_Tm',
    'Fr_Tel_No',
    'Loc_Pnt_Lat',
    'Loc_Pnt_Lng',
    'Shop_Intro',
    'Shop_No',
    'Shop_Owner_Nm',
    'Star_Pnt_Avg',
    'Tel_No',
    'Vel_No',
    'Favorite_Cnt',
    'Call_Cnt',
    'Ord_Cnt',
]
rawIds = []


def getCardList(lat, lng):
    url = 'https://api.smartbaedal.com/v2/main/popular-shops'
    headers = {
        'User-Agent': 'and1_8.18.2',
        'Carrier': '46001',
        'Device-Height': '1794',
        'Device-Width': '1080',
        'USER-BAEDAL': 'uFD1jy3fAZBHejMyftvBp0RMIBM9fgnl++RWEZgx5UIPyU8VQJMkFgqMMpLLd4Q0R4gMd+3wqhTbfMnu/xDkPnPShr4lY7bSzXdv9cgQ3ANRkqHpCC0Lj6tt/ZdjRuoidFniec7wXEXDUPVA4DGL1SevIH5hMMfTZcdxAW9AD9xOpu0qY7n7FKeA1uQOud6pMxnJ1T65buNM4qSSRdExzA==',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'api.smartbaedal.com',
        'Connection': 'Keep-Alive',
        'Content-Length': '308',
        'Accept-Encoding': 'gzip',
    }
    params = {
        'lat': lat,
        'lng': lng,
        'sessionId': '325df563ec2e31a71087843',
    }
    with requests.get(url, params=params, headers=headers) as res:
        return res.json()


def getCuisineList(lat, lng, offset, cate):
    url = 'https://api.smartbaedal.com/shop/list_normal'
    headers = {
        'User-Agent': 'and1_8.18.2',
        'Carrier': '46001',
        'Device-Height': '1794',
        'Device-Width': '1080',
        'USER-BAEDAL': 'uFD1jy3fAZBHejMyftvBp0RMIBM9fgnl++RWEZgx5UIPyU8VQJMkFgqMMpLLd4Q0R4gMd+3wqhTbfMnu/xDkPnPShr4lY7bSzXdv9cgQ3ANRkqHpCC0Lj6tt/ZdjRuoidFniec7wXEXDUPVA4DGL1SevIH5hMMfTZcdxAW9AD9xOpu0qY7n7FKeA1uQOud6pMxnJ1T65buNM4qSSRdExzA==',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'api.smartbaedal.com',
        'Connection': 'Keep-Alive',
        'Content-Length': '308',
        'Accept-Encoding': 'gzip',
    }
    params = {
        'offset': offset,
        'site': '7jWXRELC2e',
        'dvcid': 'OPUD42ad45fb97fd1ea6',
        'ct': cate,
        'sessionId': '325df563ec2e31a71087843',
        'lat': lat,
        'lng': lng,
        'kw': '',
        'ctty': '1',
        'adid': '3fc4b9af-22ff-411c-bb0e-2ab5481d8445',
        'distance': '7',
        'sort': 'default',
        'limit': '25',
    }
    with requests.post(url, data=params, headers=headers) as res:
        return res.json()


def getDetail(lat, lng, shopId):
    url = 'https://api.smartbaedal.com/shop/detail'
    headers = {
        'User-Agent': 'and1_8.10.2',
        'Carrier': '46001',
        'Device-Height': '1208',
        'Device-Width': '720',
        'USER-BAEDAL': 't6MZdflx1zzJFuBIK5eHvsmAFFUajzKvlDHsgSv+q+3YQtTTVu7IboZfDEnXbwk6HEOEndeUO5NY/F1J24DCA5xI8X+0d5uwb3L/ECYrID1GsHCUsFZLUGIUec7o6I6VTefIjjf/L/eVuFCljdkkkI1eZ+yJlBx5NKVENPjVbWyaN1J+Fv+IXcnlaieMKtzF',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'api.smartbaedal.com',
        'Connection': 'Keep-Alive',
        'Content-Length': '308',
        'Accept-Encoding': 'gzip',
    }
    params = {
        'eatout_lng': lng,
        'eatout_lat': lat,
        'site': '7jWXRELC2e',
        'defaultreview': 'N',
        'shop': shopId,
        'dvcid': 'OPUD42ad45fb97fd1ea6',
        'adid': '3fc4b9af-22ff-411c-bb0e-2ab5481d8445',
        'sessionId': '2111ad664b529363f1fe8e3a',
        'lat': lat,
        'lng': lng,
        'limit': '25',
    }
    with requests.post(url, data=params, headers=headers) as res:
        return res.json()



def eachLat():
    lat_bottom_right = 35.144152
    lng_bottom_right = 129.077963
    lat_top_left = 35.175043
    lng_top_left = 129.015926

    while lat_bottom_right < lat_top_left:
        tmp_lng = lng_top_left
        while tmp_lng < lng_bottom_right:
            saveLog('查找lat：%s,lng:%s' % (lat_bottom_right, tmp_lng))
            saveCard(lat_bottom_right, tmp_lng)
            saveCuisine(lat_bottom_right, tmp_lng)
            tmp_lng += 0.02
        lat_bottom_right += 0.02


def saveCard(lat, lng):
    try:
        list = getCardList(lat, lng)
        # print(list)
        saveLog('lat:%s,lng:%s,卡片列表获取' % (lat, lng))
    except Exception as e:
        saveLog('lat:%s,lng:%s,错误信息%s,卡片列表获取失败' % (lat, lng, e))
        return False
    data = list.get('data')
    if data is None :
        saveLog('该区域Card数据为空, 下一个')
    else:
        for shop in list.get('data', []):
            id = shop.get('id', '')
            if id in rawIds:
                saveLog('lat:%s,lng:%s,id:%s已经爬过' % (lat, lng, id))
            else:
                try:
                    shopInfo = getDetail(lat, lng, id)
                except requests.exceptions.ConnectionError as e:
                    saveLog('lat:%s,lng:%s,id:%s错误信息%s,详情获取失败' % (lat, lng, id, e))
                    break
                saveData('card', shopInfo, lat, lng)


def saveCuisine(lat, lng):
    cates = [33, 32, 10, 1, 3, 2, 4, 5, 6, 9, 34, 7]
    for cate in cates:
        offset = 0
        while True:
            try:
                saveLog('lat:%s,lng:%s,第%s页,类别：%s' % (lat, lng, offset/25, cate))
                offset += 25
                list = getCuisineList(lat, lng, offset, cate)
            except Exception as e:
                saveLog('lat:%s,lng:%s,类别：%s,第%s页,错误信息%s,菜单列表获取失败' % (lat, lng, cate, offset, e))
                return False
            list = list.get('shop_list', [])
            if len(list) == 0:
                saveLog('lat:%s,lng:%s,类别：%s,第%s页,爬完了' % (lat, lng, cate, offset/25))
                saveLog('lat:%s,lng:%s,类别%s爬完了' % (lat, lng, cate))
                break

            count = 0
            for shop in list:
                id = shop.get('Shop_No', 0)
                if not id in rawIds:
                    try:
                        shopInfo = getDetail(lat, lng, id)
                        saveLog('lat:%s,lng:%s,id:%s' % (lat, lng, id))
                        saveData('cuisine', shopInfo, lat, lng)
                    except requests.exceptions.ConnectionError as e:
                        saveLog('lat:%s,lng:%s,id:%s错误信息%s,详情获取失败' % (lat, lng, id, e))
                        break
                    except Exception as e:
                        saveLog('lat:%s,lng:%s,id:%s错误信息%s,详情获取失败' % (lat, lng, id, e))
                        break

                else:
                    saveLog('lat:%s,lng:%s,id:%s第%s页爬过了' % (lat, lng, id, offset/25))
                    count += 1


def saveData(type, data, lat, lng):
    date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    info = data.get('shop_info')
    if info is None:
        saveLog('检测到空的店铺信息')
        return False
    saveLog('lat:%s\tlng:%s\t名字:%s' % (lat, lng, info.get('Shop_Nm', '空')))

    id = info.get('Shop_No')
    if id in rawIds:
        pass
    else:
        rawIds.append(id)
        info = data.get('shop_info')
        data_statistics.append(info.get('Shop_No'))


def saveLog(str=''):
    date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logHead = [
        'ruandate',
        'str'
    ]
    logData = {
        'ruandate': date_time,
        'str': str
    }
    print('%s \t %s' % (date_time, str))
    logPath = os.path.split(logFile)
    isExists = os.path.exists(logPath[0])
    if not isExists:
        os.makedirs(logPath[0])
    with open(logFile, 'a', encoding='utf-8', newline='') as log_file:
        log_csv = csv.DictWriter(log_file, fieldnames=logHead)
        if not os.path.getsize(logFile):
            log_csv.writeheader()
        log_csv.writerow(logData)


if __name__ == '__main__':
    eachLat()
    # 10208
    if len(data_statistics)>9000:
        print('爬虫正常,原始数据10208条,实际得到%d条'%(len(data_statistics)))
    else:
        print('爬虫异常,原始数据10208条,实际得到%d条' % (len(data_statistics)))
