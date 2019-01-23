# -*- coding: utf-8 -*-
import csv
import datetime
import os
import time
import requests
import sys
from mongodb_utils import get_db

"""
1.以后维护主要考虑几个id参数是否还有效,
2.以后可能会更改爬取首尔的坐标(经纬度)
"""
# 获取mongo数据库链接对象
client_mongo=get_db()

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


def load_geodata(id):
    geo_header = [
        'name',
        'id',
        'lat_left',
        'lng_left',
        'lat_right',
        'lng_right',
    ]
    with open('input/location.csv', 'r', encoding='utf-8') as f:
        csv_f = csv.DictReader(f, fieldnames=geo_header)
        for row in csv_f:
            if csv_f.line_num == 1:
                continue
            if int(row['id']) == id:
                return dict(row)

def eachLat(id):

    geo_info = load_geodata(id)
    print(geo_info)
    lat_bottom_left = float(geo_info.get('lat_left'))
    lng_bottom_left = float(geo_info.get('lng_left'))

    lat_top_right = float(geo_info.get('lat_right'))
    lng_top_right = float(geo_info.get('lng_right'))

    while lat_bottom_left < lat_top_right:
        tmp_lng = lng_bottom_left
        while tmp_lng < lng_top_right:
            saveLog('查找lat：%s,lng:%s' % (lat_bottom_left, tmp_lng))
            saveCard(lat_bottom_left, tmp_lng)
            saveCuisine(lat_bottom_left, tmp_lng)
            tmp_lng += 0.02
        lat_bottom_left += 0.02


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
    header = [
        'Shop_No',
        'Shop_Nm',
        'Tel_No',
        'Vel_No',
        'Fr_No',
        'Fr_Tel_No',
        'Addr', 'Loc_Pnt_Lng', 'Loc_Pnt_Lat', 'Review_Cnt', 'Star_Pnt_Avg', 'Dlvry_Tm_B', 'Dlvry_Mi_B',
        'Dlvry_Tm_E', 'Dlvry_Mi_E', 'Dlvry_Date_1_B', 'Dlvry_Date_1_E', 'Dlvry_Date_2_B', 'Dlvry_Date_2_E',
        'Dlvry_Date_3_B', 'Dlvry_Date_3_E', 'Block_Date_B', 'Block_Date_E', 'Close_Date_B', 'Close_Date_E',
        'Logo_Host', 'Logo_Path', 'Logo_File', 'Dlvry_Info', 'Close_Day', 'Shop_Intro', 'Favorite_Cnt', 'View_Cnt',
        'Call_Cnt', 'Ord_Cnt', 'Ct_Cd', 'Ct_Cd_Nm', 'Ct_Cd_Nm_En', 'Ct_Ty_Cd', 'Use_Yn_Ord', 'Use_Yn_Ord_Menu',
        'Ceo_Nm', 'Biz_No', 'Shop_Owner_Nm', 'Business_Location', 'Ord_Avail_Yn', 'Svc_Shop_Ad_List', 'Shop_Icon_Cd',
        'Evt_Land_Ty_Val', 'Dh_Img_Host', 'Dh_Img_Path', 'Dh_Img_File', 'Review_Cnt_Latest', 'Review_Cnt_Ceo_Latest',
        'Review_Cnt_Ceo_Say_Latest', 'Review_Cnt_Img', 'Review_Cnt_Ceo', 'Review_Cnt_Ceo_Say',
        'Comp_No', 'Comp_Nm', 'Dh_Rgn_Ty_Cd', 'Mov_Url', 'Contract_Standard_Fee', 'Contract_Sale_Fee',
        'Contract_Sale_Fee_YN', 'Noncontract_Standard_Fee', 'Noncontract_Sale_Fee', 'Noncontract_Sale_Fee_YN',
        'Contract_Shop_Yn', 'Baemin_Kitchen_Yn', 'Shop_Prom', 'Ceo_Notice', 'Ad_Yn', 'Meet_Cash', 'Meet_Card', 'Dlvry_Tm', 'Close_Day_Tmp',
        'Award_Type','Award_Info', 'Cache', 'Live_Yn_Shop', 'Shop_Cpn_Info', 'Shop_Cpn_Yn', 'Live_Yn_Ord', 'Shop_Break_Yn',
        'Break_Tm_Info','Favorite_Yn', 'Distance', 'Distance_Txt', 'badge', 'sanitation']

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
        raw_file_name = 'raw.csv'
        rawFile = file_path + web_name + raw_file_name
        rawIds.append(id)
        rawPath = os.path.split(rawFile)
        isExists = os.path.exists(rawPath[0])
        if not isExists:
            os.makedirs(rawPath[0])
            # with open(rawFile, 'w', encoding='utf-8', newline='') as f:
            #     csv_w = csv.DictWriter(f, fieldnames=header)
            #     csv_w.writeheader()

        # with open(rawFile, 'a', encoding='utf-8', newline='') as f:
        #     raw_csv = csv.DictWriter(f, fieldnames=header)
        tmp_data = {}
        info = data.get('shop_info')
        for head in header:
            tmp_content = info.get(head)
            tmp_content = str(tmp_content).replace('\n', ' ').replace('\r', ' ')
            tmp_data[head] = tmp_content
        # raw_csv.writerow(tmp_data)
        # 将数据写入数据库
        client_mongo.insert_one('baemin',tmp_data,condition=['Shop_No'])



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
    if len(sys.argv) <2:
        id = 5
    else:
        id = int(sys.argv[1])
    print('输入参数是: ', id, type(id))
    eachLat(id)
