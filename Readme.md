2018.8.2  马晓腾
# Baemine 脚本说明
### 1. 使用python2
### 2. 定时任务cronlab中添加了 baeminApp.py, 目前只有首尔的城市
### 3. 添加新的城市
**input**: 经纬度区间
  
- 修改脚本中的经纬度, 
- 修改webname,   
```
web_name = 'baemin/Songdo/'
```

- 即可产生新的脚本


### 使用说明
```
每次启动前修改utils.py中的DATA

直接执行: sh start_baemin.sh

```

### 数据最终保存在东京0上的mongo数据库中
```
数据导出成csv文件后保存在:

 ~ /crawlerOutput/日期/baemin/

```