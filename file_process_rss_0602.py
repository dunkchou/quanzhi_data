# -*- coding:utf-8 -*- 
import os
import time
import datetime
from goose import Goose
from goose.text import StopWordsChinese
import json
import MySQLdb
from pybloomfilter import BloomFilter

g = Goose({'stopwords_class': StopWordsChinese})

# file dir
#dir = '/Users/dunkezhou/Projects/quanzhi/data/'
dir = '/alidata/data/raw_data/'


date_str = datetime.datetime.today().strftime('%Y_%m_%d')


# Bloom Filter
try:
    bf = BloomFilter.open('/root/quanzhi_ml/file_process_bloom')
except:
    bf = BloomFilter(10000000, 0.01, '/root/quanzhi_ml/file_process_bloom')


## RSS
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
                try:
                    publish_time = datetime.datetime.strptime(publish_time, "%a, %d %b %Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                except:
                    print publish_time

                first_fetch_time = config['fetch_time']
                summary = rss_info_dict.get('summary', '')
                category_dict = rss_info_dict.get('tags', [])
                if len(category_dict) > 0:
                    category = ';'.join([tmp['term'] for tmp in category_dict])
                else:
                    category = ''
                    
                
                    
                print url_id, len(body), url
                if len(body) > 0:
                    res_list.append((url_id, url, title, summary, body, '0', '0', '0', category, 
                                    '', author, publish_time, first_fetch_time, first_fetch_time, '0'))
                    
                bf.add(file_name)
                cnt += 1
            except Exception,e:
                print Exception, ':', e
                print url, file_name
                print '----------------------'


#### write file
conn= MySQLdb.connect(
        host='localhost',
        port = 3306,
        user='root',
        passwd='123@Root',
        db ='data_web',
        charset="utf8"
        )
cur = conn.cursor()



#一次插入多条记录
sqli="insert into article(id,url,title,summary,body,view_cnt,rate_cnt,vote_cnt,category,keywords,author,publish_time,first_fetch_time,last_update_time,overall_hote_rate) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
cur.executemany(sqli,res_list)

cur.close()
conn.commit()
conn.close()
