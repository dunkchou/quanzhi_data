#coding=utf-8
import urllib
import urllib2
from goose import Goose
import feedparser
from goose.text import StopWordsChinese


def extract_detail(url, g):
    article = g.extract(url=url)
    return article.cleaned_text


def extract_info(fp_entry):
    author = fp_entry.get('author', '')
    id = fp_entry.get('link', '')
    link = fp_entry.get('link', '')
    published = fp_entry.get('published', '')
    summary = fp_entry.get('summary', '')
    title = fp_entry.get('title', '')
    
    
    tags_list = []
    for tt in fp_entry.get('tags', [{}]):
        tags_list.append(tt.get('term', ''))

    
    if id == '':
        print 'Null URL'
        return {}
    else:
        return {'id':id, 'author':author, 'link':link, 'published':published, 'summary':summary, 'title':title, 'tags':'*'.join(tags_list)}
        
feed_list = []
with open('./local.txt', 'r') as f:
    for line in f.readlines():
        feed_list.append(line[1:-3])

user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0"

feed_detail_list = {}

feed_cnt = 0

for feed_url in feed_list:
    fp = feedparser.parse(feed_url, agent = user_agent)
    
    for entry in fp.entries:
        res =  extract_info(entry)
        if res:
            feed_detail_list[res['id']] = res
    
        feed_cnt += 1
        if feed_cnt % 100 ==0:
            print feed_cnt

    if feed_cnt > 10:
        break

g = Goose({'stopwords_class': StopWordsChinese, 'browser_user_agent':user_agent})
for id in feed_detail_list:
    feed_detail_list[id]['body'] = extract_detail(feed_detail_list[id]['link'], g)
    print feed_detail_list[id]['link']

print len(feed_detail_list)
import json
fp = file('test.txt', 'w')
json.dump(feed_detail_list, fp)
