# -*- coding:utf-8 -*-  

#Package
import os
import time
import datetime
from goose import Goose
from goose.text import StopWordsChinese
import json
import MySQLdb
from pybloomfilter import BloomFilter
import hashlib
import codecs
import re
from file_process_funcs import *

g = Goose({'stopwords_class': StopWordsChinese})

# file dir
dir = '/alidata/data/raw_data/'

date_str = datetime.datetime.today().strftime('%Y_%m_%d')

# check if the data file directory is available
try:
    test = os.listdir(dir + 'wechat/' + date_str)
except Exception,e:
    print Exception,':',e
    print 'File Directory Not Ready'
    exit()
    
    
# import bloom filter for filter old files
try:
    bf_wechat = BloomFilter.open('./file_process_wechat_bloom')
except:
    bf_wechat = BloomFilter(10000000, 0.01, './file_process_wechat_bloom')

# start process file
file_list = {f.split('.')[0]:1 for f in os.listdir(dir+'wechat/' + date_str)}

wechat_res_list = []

for file_name in file_list_wechat:
    time_str = ''
    f = codecs.open(dir+'wechat/' + date_str + '/' + file_name, 'r', 'gbk')
    f_text = f.readlines()
    for txt in f_text[0][12:].split('&'):
        if txt.split('=')[0] == 'signature':
            id = txt.split('=')[1][:128]
            
            id = hashlib.md5(id).hexdigest()
        if txt.split('=')[0] == 'timestamp':
            time_str = datetime.datetime.fromtimestamp(int(txt.split('=')[1])).strftime("%Y-%m-%d %H:%M:%S")
                
    title = f_text[1][14:]
    
    f_text[2] = f_text[2][13:]

    body = '\n'.join(f_text[2:])
    
     
    if id not in bf_wecaht and len(body) < 10000:
        wechat_res_list.append((id, title, body, time_str, time_str, time_str, u'微信'))                
        bf_wechat.add(id)
  
        
# Write information to sql
conn= MySQLdb.connect(
        host='localhost',
        port = 3306,
        user='root',
        passwd='123@Root',
        db ='data_web',
        charset="utf8mb4"
        )
cur = conn.cursor()



#一次插入多条记录
batch_cnt = len(wechat_res_list) / 100

for i in range(batch_cnt + 1):
    sqli="REPLACE into article(id,title,body,publish_time,first_fetch_time,last_update_time,article_fetch_type) values(%s,%s,%s,%s,%s,%s,%s)"
    cur.executemany(sqli,wechat_res_list[(i * 100): min(len(wechat_res_list), (i+1)*100)])
    print i

cur.close()
conn.commit()
conn.close()
