# -*- coding: utf-8 -*-
import csv
import datetime
import os
import time
import requests
import sys

reload(sys)
sys.setdefaultencoding('utf8')

lat_top_left = 37.71044
lng_top_left = 126.76849

lat_bottom_right = 37.42797
lng_bottom_right = 127.19009

date = datetime.datetime.now().strftime("%Y-%m-%d")
date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
limits = 500
file_path = '../crawlerOutput/' + date + '/'
web_name = 'baemin/'
raw_file_name = 'raw.csv'
cuisine_file_name = 'cuisine.csv'
deduplicate_file_name = 'deduplicate.csv'
raw_head = [
    'running_date',
    'id',
    'name',
    'owner_no',
    'address',
    'phone',
    'Use_order',
    'latitude',
    'longitude',
    'Favourites',
    'Menus',
    'Reviews_IMG',
    'Reviews_CEO',
    'Calls',
    'Orders',
    'Reviews',
    'Star',
    'Cuisine',
    'Delivery_Info',
    'Shop_info',
]
deduplicate_head=[
    "id",
    "name",
    "owner_no",
    "address",
    "phone",
    "Use_order",
    "latitude",
    "longitude",
    "Favourites",
    "Menus",
    "Reviews_IMG",
    "Reviews_CEO",
    "Calls",
    "Orders",
    "Reviews",
    "Star",
    "Cuisine",
    "Delivery_Info",
    "Shop_info",
]
raw_hash=[]

def getContent(cuisine_id, lat, lng, offset=0):
    query = {
        'carrier': 52503,
        'ct': cuisine_id,
        'ctty': 1,
        'distance': 3,
        'dvc_uniq_id': '9BD02B13-A5FB-4C87-9478-6D3D8F7C1D5E',
        'lat': lat,
        'limit': limits,
        'lng': lng,
        'offset': offset,
        'site': '7jWXRELC2e',
        'sort': 'default'
    }
    api_url = "https://api.smartbaedal.com/shop/list_normal"
    # s=requests.session()
    # s.keep_alive = False
    with requests.post(api_url,data=query,timeout=60) as response:
        print(response.url)
        jsondata = response.json()
        return jsondata


def eachLocation():
    for i in range(int(lat_bottom_right * 100), int(lat_top_left * 100)):
        for j in range(int(lng_top_left * 100), int(lng_bottom_right * 100)):
            time.sleep(1)
            lat = float(i) / 100
            lng = float(j) / 100
            print 'lat:', lat, 'lng:', lng
            for cuisine_id in range(1, 40):
                jsondict = getContent(cuisine_id, lat, lng)
                total_resto = getDict(jsondict, 'list_info.totalCount', 0)
                for offest in range(1, total_resto, limits):
                    jsondict = getContent(cuisine_id, lat, lng, offest)
                    total_resto = getDict(jsondict, 'list_info.totalCount', 0)
                    saveLog(lat,lng,cuisine_id,total_resto,len(getDict(jsondict,'shop_list',{})))
                    print('lat:%s,lng:%s,cuisine_id:%d,total_resto:%d,length:%d' % (
                        lat, lng, cuisine_id, total_resto, len(getDict(jsondict, 'shop_list', {}))))
                    for shop in jsondict['shop_list']:
                        raw_data = {
                            'running_date': date,
                            'id': 'NA' if getDict(shop, 'Shop_No', None) is None else getDict(shop,'Shop_No',None),
                            'name': 'NA' if getDict(shop, 'Shop_Nm', None) is None else getDict(shop,'Shop_Nm',None),
                            'owner_no': 'NA' if getDict(shop, 'Shop_Owner_No', None) is None else getDict(shop,'Shop_Owner_No',None),
                            'address': 'NA' if getDict(shop, 'Addr', None) is None else getDict(shop,'Addr',None),
                            'phone': 'NA' if getDict(shop, 'Tel_No', None) is None else getDict(shop,'Tel_No',None),
                            'Use_order': 'NA' if getDict(shop, 'Use_Yn_Ord', None) is None else getDict(shop,'Use_Yn_Ord',None),
                            'latitude': 'NA' if getDict(shop, 'Loc_Pnt_Lat', None) is None else getDict(shop,'Loc_Pnt_Lat',None),
                            'longitude': 'NA' if getDict(shop, 'Loc_Pnt_Lng', None) is None else getDict(shop,'Loc_Pnt_Lng',None),
                            'Favourites': 'NA' if getDict(shop, 'Favorite_Cnt', None) is None else getDict(shop,'Favorite_Cnt',None),
                            'Menus': 'NA' if getDict(shop, 'Shop_Menu_Cnt', None) is None else getDict(shop,'Shop_Menu_Cnt',None),
                            'Reviews_IMG': 'NA' if getDict(shop, 'Review_Cnt_Img', None) is None else getDict(shop,'Review_Cnt_Img',None),
                            'Reviews_CEO': 'NA' if getDict(shop, 'Review_Cnt_Ceo', None) is None else getDict(shop,'Review_Cnt_Ceo',None),
                            'Calls': 'NA' if getDict(shop, 'Call_Cnt', None) is None else getDict(shop,'Call_Cnt',None),
                            'Orders': 'NA' if getDict(shop, 'Ord_Cnt', None) is None else getDict(shop,'Ord_Cnt',None),
                            'Reviews': 'NA' if getDict(shop, 'Review_Cnt', None) is None else getDict(shop,'Review_Cnt',None),
                            'Star': 'NA' if getDict(shop, 'Star_Pnt_Avg', None) is None else getDict(shop,'Star_Pnt_Avg',None),
                            'Cuisine': 'NA' if getDict(shop, 'Ct_Cd_Nm_En', None) is None else getDict(shop,'Ct_Cd_Nm_En',None),
                            'Delivery_Info': 'NA' if getDict(shop, 'Dlvry_Info', None) is None else getDict(shop,'Dlvry_Info',None),
                            'Shop_info': 'NA' if getDict(shop, 'Shop_Intro', None) is None else getDict(shop,'Shop_Intro',None).replace('\r\n','').replace('\n',''),
                        }
                        isExists = os.path.exists(file_path +web_name)
                        if not isExists:
                            os.makedirs(file_path +web_name)
                        raw_file_path=file_path+web_name+raw_file_name
                        with open(raw_file_path,'ab') as raw_file:
                            raw_csv=csv.DictWriter(raw_file,fieldnames=raw_head)
                            if (not os.path.getsize(raw_file_path)):
                                raw_csv.writeheader()
                            raw_csv.writerow(raw_data)
                            has_str=''
                            for key, value in raw_data.items():
                                has_str+="\"%s\":\"%s\"" % (key, value)
                            raw_hash_data=hash(has_str)
                            if (not raw_hash_data in raw_hash):
                                raw_hash.append(raw_hash_data)
                                deduplicate_data={
                                    "id": raw_data['id'],
                                    "name": raw_data['name'],
                                    "owner_no": raw_data['owner_no'],
                                    "address": raw_data['address'],
                                    "phone": raw_data['phone'],
                                    "Use_order": raw_data['Use_order'],
                                    "latitude": raw_data['latitude'],
                                    "longitude": raw_data['longitude'],
                                    "Favourites": raw_data['Favourites'],
                                    "Menus": raw_data['Menus'],
                                    "Reviews_IMG": raw_data['Reviews_IMG'],
                                    "Reviews_CEO": raw_data['Reviews_CEO'],
                                    "Calls": raw_data['Calls'],
                                    "Orders": raw_data['Orders'],
                                    "Reviews": raw_data['Reviews'],
                                    "Star": raw_data['Star'],
                                    "Cuisine": raw_data['Cuisine'],
                                    "Delivery_Info": raw_data['Delivery_Info'],
                                    "Shop_info": raw_data['Shop_info'],
                                }
                                deduplicate_file_path = file_path + web_name + deduplicate_file_name
                                with open(deduplicate_file_path, 'ab') as deduplicate_file:
                                    deduplicate_csv = csv.DictWriter(deduplicate_file, fieldnames=deduplicate_head)
                                    if (not os.path.getsize(deduplicate_file_path)):
                                        deduplicate_csv.writeheader()
                                    deduplicate_csv.writerow(deduplicate_data)

def getDict(dict, key, default):
    keys = key.strip('.').split('.')
    try:
        for i in keys:
            dict = dict[i]
        return dict
    except KeyError:
        return default

def saveLog(lat,lng,cuisine_id, total_resto,length):
    date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('run_time:%s,lat:%s,lng:%s,cuisine_id:%d,total_resto:%d,length:%d' % (
        date_time,lat, lng, cuisine_id, total_resto, length))
    log_data={
        'rundate':date_time,
        'lat':lat,
        'lng':lng,
        'cuisine_id':cuisine_id,
        'total_resto':total_resto,
        'length':length
    }
    isExists = os.path.exists(file_path+'log/'+web_name)
    if not isExists:
        os.makedirs(file_path+'log/'+web_name)
    log_file_path = file_path + 'log/' + web_name + 'log.csv'
    with open(log_file_path, 'ab') as log_file:
        log_csv = csv.DictWriter(log_file, fieldnames=['rundate','lat','lng','cuisine_id','total_resto','length'])
        if (not os.path.getsize(log_file_path)):
            log_csv.writeheader()
        log_csv.writerow(log_data)

if __name__ == '__main__':
    eachLocation()
