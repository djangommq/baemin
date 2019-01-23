import os
import csv
import datetime
from mongodb_utils import get_db

date = datetime.datetime.now().strftime("%Y-%m-%d")
file_path = os.path.dirname(os.path.split(os.path.realpath(__file__))[0]) + '/crawlerOutput/' + date + '/'
web_name = 'baemin/'

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
    'Contract_Sale_Fee_YN','Noncontract_Standard_Fee', 'Noncontract_Sale_Fee', 'Noncontract_Sale_Fee_YN', 'Contract_Shop_Yn',
    'Baemin_Kitchen_Yn','Shop_Prom', 'Ceo_Notice', 'Ad_Yn', 'Meet_Cash', 'Meet_Card', 'Dlvry_Tm', 'Close_Day_Tmp', 'Award_Type',
    'Award_Info','Cache', 'Live_Yn_Shop', 'Shop_Cpn_Info', 'Shop_Cpn_Yn', 'Live_Yn_Ord', 'Shop_Break_Yn', 'Break_Tm_Info',
    'Favorite_Yn','Distance', 'Distance_Txt', 'badge', 'sanitation']


if __name__ == '__main__':
    # 创建链接mongo数据库对象
    mongoclient=get_db()
    # 获取所有的数据
    data_info=mongoclient.all_items('baemin')

    # 将数据保存至csv文件
    raw_file_name = 'raw.csv'
    rawFile = file_path + web_name + raw_file_name

    rawpath=file_path + web_name
    if not os.path.exists(rawpath):
        os.makedirs(rawpath)

    with open(rawFile,'a',encoding='utf-8',newline='')as f:
          writer=csv.DictWriter(f,fieldnames=header)
          if not os.path.getsize(rawFile):
                writer.writeheader()
          for data in data_info:
              writer.writerow(data)

    print('成功导出')