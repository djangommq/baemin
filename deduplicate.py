import csv
import datetime
import os

# rawHead = [
#     'rundate',
#     'type',
#     'Shop_Nm',
#     'Addr',
#     'Biz_No',
#     'Close_Day',
#     'Ct_Cd',
#     'Ct_Cd_Nm',
#     'Ct_Cd_Nm_En',
#     'Dlvry_Info',
#     'Dlvry_Tm',
#     'Fr_Tel_No',
#     'Loc_Pnt_Lat',
#     'Loc_Pnt_Lng',
#     'Shop_Intro',
#     'Shop_No',
#     'Shop_Owner_Nm',
#     'Star_Pnt_Avg',
#     'Tel_No',
#     'Vel_No',
#     'Favorite_Cnt',
#     'Call_Cnt',
#     'Ord_Cnt',
# ]

rawHead = [
        'Shop_No', 
        'Shop_Nm', 
        'Tel_No', 
        'Vel_No', 
        'Fr_No', 
        'Fr_Tel_No', 
        'Addr', 'Loc_Pnt_Lng', 'Loc_Pnt_Lat', 'Review_Cnt', 'Star_Pnt_Avg', 'Dlvry_Tm_B', 'Dlvry_Mi_B',
        'Dlvry_Tm_E', 'Dlvry_Mi_E', 'Dlvry_Date_1_B', 'Dlvry_Date_1_E', 'Dlvry_Date_2_B', 'Dlvry_Date_2_E', 
        'Dlvry_Date_3_B', 'Dlvry_Date_3_E', 'Block_Date_B', 'Block_Date_E', 'Close_Date_B', 'Close_Date_E', 
        'Logo_Host', 'Logo_Path', 'Logo_File','Dlvry_Info', 'Close_Day', 'Shop_Intro', 'Favorite_Cnt', 'View_Cnt', 
        'Call_Cnt', 'Ord_Cnt', 'Ct_Cd', 'Ct_Cd_Nm', 'Ct_Cd_Nm_En', 'Ct_Ty_Cd', 'Use_Yn_Ord', 'Use_Yn_Ord_Menu', 
        'Ceo_Nm', 'Biz_No', 'Shop_Owner_Nm', 'Business_Location', 'Ord_Avail_Yn', 'Svc_Shop_Ad_List', 'Shop_Icon_Cd', 
        'Evt_Land_Ty_Val', 'Dh_Img_Host', 'Dh_Img_Path', 'Dh_Img_File', 'Review_Cnt_Latest', 'Review_Cnt_Ceo_Latest', 
        'Review_Cnt_Ceo_Say_Latest', 'Review_Cnt_Img', 'Review_Cnt_Ceo', 'Review_Cnt_Ceo_Say',
        'Comp_No', 'Comp_Nm', 'Dh_Rgn_Ty_Cd', 'Mov_Url', 'Contract_Standard_Fee', 'Contract_Sale_Fee', 'Contract_Sale_Fee_YN', 
        'Noncontract_Standard_Fee', 'Noncontract_Sale_Fee', 'Noncontract_Sale_Fee_YN', 'Contract_Shop_Yn', 'Baemin_Kitchen_Yn', 
        'Shop_Prom', 'Ceo_Notice', 'Ad_Yn', 'Meet_Cash', 'Meet_Card', 'Dlvry_Tm', 'Close_Day_Tmp', 'Award_Type', 'Award_Info', 
        'Cache', 'Live_Yn_Shop', 'Shop_Cpn_Info','Shop_Cpn_Yn', 'Live_Yn_Ord', 'Shop_Break_Yn', 'Break_Tm_Info', 'Favorite_Yn', 
        'Distance', 'Distance_Txt', 'badge', 'sanitation']

def deduplicate_csv():
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    file_dir = os.path.dirname(os.path.split(os.path.realpath(__file__))[0]) + '/crawlerOutput/' + date + '/baemin/'
    file_name = "raw.csv"
    new_name = "deduplicate.csv"
    file_path = os.path.join(file_dir, file_name)
    new_path = os.path.join(file_dir, new_name)
    with open('tmp.csv', 'w', encoding='utf-8') as fw:
            new_lines = []
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    if '\0' in line:
                        new_line = line.replace('\0','')
                        new_lines.append(new_line)
                    else:
                        new_lines.append(line)
            fw.writelines(new_lines)

    rid_list = []
    with open(new_path, 'w', newline='', encoding='utf-8') as f:
        csv_writer = csv.DictWriter(f, fieldnames=rawHead)
        csv_writer.writeheader()
        with open('tmp.csv', 'r', newline='', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f, fieldnames=rawHead)
            for r in csv_reader:
                if csv_reader.line_num == 1:
                    continue
                r_id = r.get('Shop_No')
                if r_id in rid_list:
                    continue
                else:
                    rid_list.append(r_id)
                    csv_writer.writerow(r)


if __name__ == "__main__":
    deduplicate_csv()
