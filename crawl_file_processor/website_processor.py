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
from bs4 import BeautifulSoup

month_map = {'一月':'01', '二月':'02',
             '三月':'03','四月':'04',
             '五月':'05','六月':'06',
             '七月':'07','八月':'08',
             '九月':'09','十月':'10',
             '十一月':'11','十二月':'12',
            }



g = Goose({'stopwords_class': StopWordsChinese})

# file dir
dir = '/alidata/data/raw_data/'

date_str = datetime.datetime.today().strftime('%Y_%m_%d')

# check if the data file directory is available
try:
    test = os.listdir(dir + 'website/' + date_str)
except Exception,e:
    print Exception,':',e
    print 'File Directory Not Ready'
    exit()
    
    
# import bloom filter for filter old files
try:
    bf_website = BloomFilter.open('./file_process_website_bloom')
except:
    bf_website = BloomFilter(10000000, 0.01, './file_process_website_bloom')

# start process file
file_list = {f.split('.')[0]:1 for f in os.listdir(dir+'website/' + date_str)}

website_res_list = []

for file_name in file_list:
    if file_name not in bf_website:
        config = json.load(open(dir+'website/' + date_str + '/' + file_name + '.json', 'r'))
        with open(dir+'website/' + date_str + '/' + file_name + '.html', 'r') as f:
            html_str = ' '.join(f.readlines())
            res = parse_html(html_str, config['source'], config['url'])

            if res:
                res += [config['id'], config['url'], config['source'], config['fetch_time'], '网站']

                website_res_list.append(res)
                #[id, url, source, first_fetch_time, last_update_time, article_fetch_type, title, author, publish_time, view_cnt, summary, body, comment_cnt]
                bf_website.add(file_name)

                
for i in range(len(website_res_list)):
    website_res_list[i][3] = website_res_list[i][3].encode('utf-8')
    if website_res_list[i][3] == '':
        website_res_list[i][3] = '0'
    if website_res_list[i][6] == '':
        website_res_list[i][6] = '0' 
        
    if len(website_res_list[i][5]) > 20000:
        website_res_list[i][5] = website_res_list[i][5][:20000]        
        
        
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
batch_cnt = len(website_res_list) / 100

for i in range(batch_cnt + 1):
    sqli="REPLACE into article( title, author, publish_time, view_cnt, summary, body, vote_cnt,id, url, source, first_fetch_time, article_fetch_type) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cur.executemany(sqli,website_res_list[(i * 100): min(len(website_res_list), (i+1)*100)])
    print i

cur.close()
conn.commit()
conn.close()
