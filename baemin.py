# -*- coding: utf-8 -*-

import csv
import datetime
import json
import math
import sys
import urllib

lat_top_left = 37.71044
lng_top_left = 126.76849

lat_bottom_right = 37.42797
lng_bottom_right = 127.19009

limits = 500
cnt = 0
n = 0

total = math.floor(100 * (lat_top_left - lat_bottom_right)) * math.floor(100 * (lng_bottom_right - lng_top_left))

date = datetime.datetime.now().strftime("%Y-%m-%d")
if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    api_url = 'http://api.smartbaedal.com/shop/list_normal'
    with open(date + '_baemin.csv', 'wb') as csvfile:
        fieldnames = ['running_date',
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
                      'Shop_info']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(int(lat_bottom_right * 100), int(lat_top_left * 100)):
            for j in range(int(lng_top_left * 100), int(lng_bottom_right * 100)):
                lat = float(i) / 100
                lng = float(j) / 100
                print 'lat:', lat, 'lng:', lng
                for cuisine_id in range(1, 40):
                    url_params = {'carrier': 52503, 'ct': cuisine_id, 'ctty': 1, 'distance': 3,
                                  'dvc_uniq_id': '9BD02B13-A5FB-4C87-9478-6D3D8F7C1D5E', 'lat': lat,
                                  'limit': limits, 'lng': lng, 'offset': 0, 'site': '7jWXRELC2e',
                                  'sort': 'default'}
                    print "url params:", url_params
                    url = api_url + '?' + urllib.urlencode(url_params)
                    response = urllib.urlopen(url)
                    data = json.loads(response.read())
                    if 'list_info' not in data:
                        print "ERROR data:", data
                        continue
                    total_count = data['list_info']['totalCount']
                    print "total count:", total_count
                    for offs in range(0, total_count, limits):
                        url_params = {'carrier': 52503, 'ct': cuisine_id, 'ctty': 1, 'distance': 3,
                                      'dvc_uniq_id': '9BD02B13-A5FB-4C87-9478-6D3D8F7C1D5E', 'lat': lat,
                                      'limit': limits, 'lng': lng, 'offset': offs, 'site': '7jWXRELC2e',
                                      'sort': 'default'}
                        print "url params with offset:", url_params
                        url = api_url + '?' + urllib.urlencode(url_params)
                        response = urllib.urlopen(url)
                        data = json.loads(response.read())
                        if 'shop_list' in data and len(data['shop_list']) > 0:
                            for item in data['shop_list']:
                                row = {}
                                row['running_date'] = date
                                row['id'] = item['Shop_No'] if 'Shop_No' in item else "NA"
                                row['name'] = item['Shop_Nm'] if 'Shop_Nm' in item else "NA"
                                row['owner_no'] = item['Shop_Owner_No'] if 'Shop_Owner_No' in item else "NA"
                                row['address'] = item['Addr'] if 'Addr' in item else "NA"
                                row['phone'] = item['Tel_No'] if 'Tel_No' in item else "NA"
                                row['Use_order'] = item['Use_Yn_Ord'] if 'Use_Yn_Ord' in item else "NA"
                                row['latitude'] = item['Loc_Pnt_Lat'] if 'Loc_Pnt_Lat' in item else "NA"
                                row['longitude'] = item['Loc_Pnt_Lng'] if 'Loc_Pnt_Lng' in item else "NA"
                                row['Favourites'] = item['Favorite_Cnt'] if 'Favorite_Cnt' in item else "NA"
                                row['Menus'] = item['Shop_Menu_Cnt'] if 'Shop_Menu_Cnt' in item else "NA"
                                row['Reviews_IMG'] = item['Review_Cnt_Img'] if 'Review_Cnt_Img' in item else "NA"
                                row['Reviews_CEO'] = item['Review_Cnt_Ceo'] if 'Review_Cnt_Ceo' in item else "NA"
                                row['Calls'] = item['Call_Cnt'] if 'Call_Cnt' in item else "NA"
                                row['Orders'] = item['Ord_Cnt'] if 'Ord_Cnt' in item else "NA"
                                row['Reviews'] = item['Review_Cnt'] if 'Review_Cnt' in item else "NA"
                                row['Star'] = item['Star_Pnt_Avg'] if 'Star_Pnt_Avg' in item else "NA"
                                row['Cuisine'] = item['Ct_Cd_Nm_En'] if 'Ct_Cd_Nm_En' in item else "NA"
                                row['Delivery_Info'] = item['Dlvry_Info'] if 'Dlvry_Info' in item else "NA"
                                row['Shop_info'] = item['Shop_Intro'] if 'Shop_Intro' in item else "NA"
                                writer.writerow(row)
