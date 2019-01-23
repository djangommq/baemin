# -*- coding: utf-8 -*-
import csv
import datetime
import os
import time
import requests
import sys

"""
1.以后维护主要考虑几个id参数是否还有效,
2.以后可能会更改爬取首尔的坐标(经纬度)
"""

reload(sys)
sys.setdefaultencoding('utf8')

date = datetime.datetime.now().strftime("%Y-%m-%d")
file_path = os.path.dirname(os.path.split(os.path.realpath(__file__))[0]) + '/crawlerOutput/' + date + '/'
print(file_path)
web_name = 'baemin/Bundang/'
raw_file_name = 'raw.csv'
rawFile = file_path + web_name + raw_file_name
logFile = file_path + 'log/' + web_name + 'log.csv'
deduplicateFile = file_path + web_name + 'deduplicate.csv'
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
        'lat': lat,
        'lng': lng,
        'sessionId': '2111ad664b529363f1fe8e3a',
    }
    with requests.get(url, params=params, headers=headers) as res:
        return res.json()


def getCuisineList(lat, lng, offset, cate):
    url = 'https://api.smartbaedal.com/shop/list_normal'
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
        'offset': offset,
        'site': '7jWXRELC2e',
        'dvcid': 'OPUD42ad45fb97fd1ea6',
        'ct': cate,
        'sessionId': '2111ad664b529363f1fe8e3a',
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
    lat_top_left = 37.418062
    lng_top_left = 127.018399

    # lat_bottom_right = 37.59301
    # lng_bottom_right = 127.19009

    lat_bottom_right = 37.335393
    lng_bottom_right = 127.179215
    while lat_bottom_right < lat_top_left:
        while lng_bottom_right > lng_top_left:
            saveLog('查找lat：%s,lng:%s' % (lat_bottom_right, lng_bottom_right))
            saveCard(lat_bottom_right, lng_bottom_right)
            saveCuisine(lat_bottom_right, lng_bottom_right)
            lng_bottom_right -= 0.05
        lng_bottom_right = 127.179215
        lat_bottom_right += 0.01


def saveCard(lat, lng):
    try:
        list = getCardList(lat, lng)
        print(list)
        saveLog('lat:%s,lng:%s,卡片列表获取' % (lat, lng))
    except Exception, e:
        saveLog('lat:%s,lng:%s,错误信息%s,卡片列表获取失败' % (lat, lng, e.message))
        return False
    for shop in list.get('data', {}):
        id = shop.get('id', '')
        if id in rawIds:
            saveLog('lat:%s,lng:%s,id:%s已经爬过' % (lat, lng, id))
        else:
            try:
                shopInfo = getDetail(lat, lng, id)
            except requests.exceptions.ConnectionError, e:
                saveLog('lat:%s,lng:%s,id:%s错误信息%s,详情获取失败' % (lat, lng, id, e.message))
                break
            saveData('card', shopInfo, lat, lng)


def saveCuisine(lat, lng):
    cates = [33, 32, 10, 1, 3, 2, 4, 5, 6, 9, 34, 7]
    for cate in cates:
        offset = 0
        isWhile = True
        while isWhile:
            try:
                saveLog('lat:%s,lng:%s,第%s页,类别：%s' % (lat, lng, offset/25, cate))
                offset += 25
                list = getCuisineList(lat, lng, offset, cate)
            except Exception, e:
                saveLog('lat:%s,lng:%s,类别：%s,第%s页,错误信息%s,菜单列表获取失败' % (lat, lng, cate, offset, e.message))
                return False
            list = list.get('shop_list', {})
            if not len(list):
                saveLog('lat:%s,lng:%s,类别：%s,第%s页,爬完了' % (lat, lng, cate, offset/25))
                saveLog('lat:%s,lng:%s,类别%s爬完了' % (lat, lng, cate))
                isWhile = False
                break

            listLen = len(list)
            count = 0
            for shop in list:
                id = shop.get('Shop_No', 0)
                if not id in rawIds:
                    try:
                        shopInfo = getDetail(lat, lng, id)
                        saveLog('lat:%s,lng:%s,id:%s' % (lat, lng, id))
                        saveData('cuisine', shopInfo, lat, lng)
                    except requests.exceptions.ConnectionError, e:
                        saveLog('lat:%s,lng:%s,id:%s错误信息%s,详情获取失败' % (lat, lng, id, e.message))
                        break
                    except Exception, e:
                        saveLog('lat:%s,lng:%s,id:%s错误信息%s,详情获取失败' % (lat, lng, id, e.message))
                        break
                    
                else:
                    saveLog('lat:%s,lng:%s,id:%s第%s页爬过了' % (lat, lng, id, offset/25))
                    count += 1



def saveData(type, data, lat, lng):
    date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    info = data.get('shop_info', {})
    if not len(info):
        saveLog('检测到空的店铺信息')
        return False
    saveLog('lat:%s\tlng:%s\t名字:%s' % (lat, lng, info.get('Shop_Nm', '空')))
    rawData = {
        'rundate': date_time,
        'type': type,
        'Addr': info.get('Addr', 'NA'),
        'Biz_No': info.get('Biz_No', 'NA'),
        'Close_Day': info.get('Close_Day', 'NA'),
        'Ct_Cd': info.get('Ct_Cd', 'NA'),
        'Ct_Cd_Nm': info.get('Ct_Cd_Nm', 'NA'),
        'Ct_Cd_Nm_En': info.get('Ct_Cd_Nm_En', 'NA'),
        'Dlvry_Info': info.get('Dlvry_Info', 'NA'),
        'Dlvry_Tm': info.get('Dlvry_Tm', 'NA'),
        'Fr_Tel_No': info.get('Fr_Tel_No', 'NA'),
        'Loc_Pnt_Lat': info.get('Loc_Pnt_Lat', 'NA'),
        'Loc_Pnt_Lng': info.get('Loc_Pnt_Lng', 'NA'),
        'Shop_Intro': info.get('Shop_Intro', 'NA'),
        'Shop_Nm': info.get('Shop_Nm', 'NA'),  # 店名
        'Shop_No': info.get('Shop_No', 'NA'),
        'Shop_Owner_Nm': info.get('Shop_Owner_Nm', 'NA'),
        'Star_Pnt_Avg': info.get('Star_Pnt_Avg', 'NA'),
        'Tel_No': info.get('Tel_No', 'NA'),
        'Vel_No': info.get('Vel_No', 'NA'),
        'Favorite_Cnt': info.get('Favorite_Cnt', 'NA'),
        'Call_Cnt': info.get('Call_Cnt', 'NA'),
        'Ord_Cnt': info.get('Ord_Cnt', 'NA'),  # 销量是call_cnt+ord_cnt
    }
    rawPath = os.path.split(rawFile)
    isExists = os.path.exists(rawPath[0])
    if not isExists:
        os.makedirs(rawPath[0])
    with open(rawFile, 'ab') as raw_file:
        raw_csv = csv.DictWriter(raw_file, fieldnames=rawHead)
        if not os.path.getsize(rawFile):
            raw_csv.writeheader()
        raw_csv.writerow(rawData)
    id = info.get('Shop_No', 'NA')
    if id not in rawIds:
        rawIds.append(id)
        deduPath = os.path.split(deduplicateFile)
        isExists = os.path.exists(deduPath[0])
        if not isExists:
            os.makedirs(deduPath[0])
        with open(deduplicateFile, 'ab') as dedu_file:
            dedu_csv = csv.DictWriter(dedu_file, fieldnames=rawHead)
            if not os.path.getsize(deduplicateFile):
                dedu_csv.writeheader()
            dedu_csv.writerow(rawData)


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
    with open(logFile, 'ab') as log_file:
        log_csv = csv.DictWriter(log_file, fieldnames=logHead)
        if not os.path.getsize(logFile):
            log_csv.writeheader()
        log_csv.writerow(logData)


if __name__ == '__main__':
    eachLat()
