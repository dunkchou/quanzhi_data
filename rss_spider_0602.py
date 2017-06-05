##coding=utf-8                                                                                                                                                                 

import os                                        
import urllib
import urllib2
import feedparser
import json
import hashlib
from pybloomfilter import BloomFilter
import datetime
import time

try:
    bf = BloomFilter.open('/root/quanzhi_ml/rss_bloom')
except:
    bf = BloomFilter(10000000, 0.01, '/root/quanzhi_ml/rss_bloom')

## Readin Feed List
feed_list = []
with open('./local.txt', 'r') as f:
    for line in f.readlines():
        feed_list.append(line[1:-3])

## Set up spider parameter
user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0"
feed_detail_list = {}
article_cnt = 1


date_str = datetime.datetime.today().strftime('%Y_%m_%d')
output_dir = '/alidata/data/raw_data/rss/' 

if output_dir + date_str not in os.listdir(output_dir):
    os.makedirs(output_dir + date_str)
    
## Readin history url information: using bloom Filter
for feed_url in feed_list:
    fp = feedparser.parse(feed_url, agent = user_agent)
    print feed_url
    print '--------------------------'
    for entry in fp.entries[:20]:
        if 'published_parsed' in entry:
            del entry['published_parsed']
        if 'updated_parsed' in entry:
            del entry['updated_parsed']

        link_md5 =hashlib.md5(entry['link']).hexdigest()
        if link_md5 not in bf:
            try:
                request = urllib2.Request(entry['link'], None, { 'User-Agent': user_agent })
                response = urllib2.urlopen(request)                

                if response.getcode()!= 200: 
                    print None
                else:
                    with open(output_dir + date_str + '/' +  link_md5 + '.html', 'w') as f:
                        f.write(response.read())
                    article_cnt += 1

                    with open(output_dir + date_str + '/' +  link_md5 + '.json', 'w') as f:
                        json.dump({'id':link_md5,
                                       'source':'rss',
                                       'rss_item_info':json.dumps(entry),
                                       'url':entry['link'],
                                       'fetch_time':datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')},
                                     f)                 
                    bf.add(link_md5)
            except Exception,e:
                print Exception, ':', e
                print entry['link']
                print '----------------------------'
                    
            time.sleep(0.1)
    if article_cnt % 100 ==0:
        print article_cnt

