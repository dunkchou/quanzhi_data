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

#g = Goose({'stopwords_class': StopWordsChinese})
g = Goose({'stopwords_class': StopWordsChinese, 'http_timeout':2, 'enable_image_fetching':False})


# file dir
dir = '/alidata/data/raw_data/'

date_str = datetime.datetime.today().strftime('%Y_%m_%d')

# check if the data file directory is available
try:
    test = os.listdir(dir + 'rss/' + date_str)
except Exception,e:
    print Exception,':',e
    print 'File Directory Not Ready'
    exit()
    
    
# import bloom filter for filter old files
try:
    bf = BloomFilter.open('./file_process_bloom')
except:
    bf = BloomFilter(10000000, 0.01, './file_process_bloom')

    
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

# start process file
file_list = {f.split('.')[0]:1 for f in os.listdir(dir+'rss/' + date_str)}

res_list = []
cnt = 0
for file_name in file_list:
    if file_name not in bf:
        with open(dir+'rss/' + date_str + '/' + file_name + '.html', 'r') as f:
            try:
                html_str = ' '.join(f.readlines())
                article = g.extract(raw_html=html_str)
                body = article.cleaned_text
                url_id = file_name

                config = json.load(open(dir+'rss/' + date_str + '/' + file_name + '.json', 'r'))

                url = config['url']

                rss_info_dict = json.loads(config['rss_item_info'])
                title = rss_info_dict['title']
                if len(rss_info_dict.get('authors', [])) > 0:
                    author = rss_info_dict['authors'][0].get('name', '')
                else:
                    author = ''

                publish_time = rss_info_dict.get('published', rss_info_dict.get('updated', config['fetch_time']))
                publish_time = time_process_func(publish_time)

                first_fetch_time = config['fetch_time']
                summary = rss_info_dict.get('summary', '')
                category_dict = rss_info_dict.get('tags', [])
                if len(category_dict) > 0:
                    category = ';'.join([tmp['term'] for tmp in category_dict])
                else:
                    category = ''
                
                url_lst = url.split('//')
                if len(url_lst) > 1:
                    source = url_lst[1].split('/')[0]
                else:
                    source = url_lst[0].split('/')[0]
                    
                #print url_id, len(body), url
                if len(body) > 0:
                    res_list.append((url_id, url, title, summary, body, '0', '0', '0', category, 
                                    '', author, publish_time, first_fetch_time, first_fetch_time, '0', source ,'RSS'))
                    
                    sqli="REPLACE into article(id,url,title,summary,body,view_cnt,rate_cnt,vote_cnt,keywords,category,author,publish_time,first_fetch_time,last_update_time,overall_hote_rate,source,article_fetch_type) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cur.execute(sqli,(url_id, url, title, summary, body, '0', '0', '0', category, 
                                        '', author, publish_time, first_fetch_time, first_fetch_time, '0', source ,'RSS'))

                bf.add(file_name)
                cnt += 1
                if cnt % 100 == 0:
                    print(cnt)
            except Exception,e:
                print Exception, ':', e
                print url, file_name, publish_time
                print '----------------------'


#一次插入多条记录
'''
batch_cnt = len(res_list) / 100

for i in range(batch_cnt + 1):
    sqli="REPLACE into article(id,url,title,summary,body,view_cnt,rate_cnt,vote_cnt,keywords,category,author,publish_time,first_fetch_time,last_update_time,overall_hote_rate,source,article_fetch_type) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cur.executemany(sqli,res_list[(i * 100): min(len(res_list), (i+1)*100)])
'''
cur.close()
conn.commit()
conn.close()
