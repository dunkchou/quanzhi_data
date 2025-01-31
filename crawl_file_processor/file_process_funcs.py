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
from bs4 import BeautifulSoup
month_map = {'一月':'01', '二月':'02',
             '三月':'03','四月':'04',
             '五月':'05','六月':'06',
             '七月':'07','八月':'08',
             '九月':'09','十月':'10',
             '十一月':'11','十二月':'12',
            }

def time_process_func(time_str):
    try:
        publish_time = datetime.datetime.strptime(time_str, "%a, %d %b %Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        return publish_time
    except:
        pass
    
    try:
        publish_time = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        return publish_time        
    except:
        pass
        
    try:
        publish_time = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M:%S")
        return publish_time        
    except:
        pass
                
    try:
        publish_time = datetime.datetime.strptime(time_str[:-6], "%a, %d %b %Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        return publish_time        
    except:
        pass
        
    try:
        publish_time = datetime.datetime.strptime(time_str[:10] + ' ' + time_str[11:19], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        return publish_time        
    except:
        pass
        
    try:
        publish_time = datetime.datetime.strptime(time_str[:-2], "%a, %d %b %Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        return publish_time      
    except:
        pass   
        
    try:
        publish_time  = datetime.datetime.strptime(time_str, "%Y年%m月%d日 %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        return publish_time
    except:
        pass

    try:
        publish_time  = datetime.datetime.strptime(time_str, "%Y年%m月%d日 %H:%M").strftime("%Y-%m-%d %H:%M:%S")
        return publish_time   
    except:
        pass
        
    try:
        publish_time  = datetime.datetime.strptime(time_str, "%Y年%m月%d日").strftime("%Y-%m-%d %H:%M:%S")
        return publish_time  
    except:
        pass


    
def parse_html(html_str, source, domain_url):
    a = BeautifulSoup(html_str, 'html.parser')
    if html_str == '':
        return None
    
    if source == u'安全牛':
        title = a.h2.text
        author = a.find(class_='author').a.text
        publish_time_str = a.find(class_='date').text  
        publish_time = '-'.join([publish_time_str.split(',')[2].strip(), month_map[publish_time_str.split(',')[1].strip().split(' ')[0].encode('utf-8')], publish_time_str.split(',')[1].split(' ')[-1].strip()]) + ' 00:00:00'
        comment_cnt = a.find(class_='comments').text
        tmp = a.find_all(class_='blog-excerpt')   
        body = '\n'.join([txt.text for txt in  tmp[0].find_all('p')])
        summary = ''
        view_cnt = ''
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]



    if source == 'freebuf':
        try:
            title = a.h2.text.strip()
        except:
            print 'title Not found'
            title = ''
        
        try:
            author = a.find(class_='property').find(class_='name').text.strip()
        except:
            author = ''
            print 'author not found', domain_url
            
        try:
            publish_time = a.find(class_='property').find(class_='time').text.strip() + ' 00:00:00'
        except:
            publish_time = ''
            print 'publish time not found', domain_url
            
        try:
            view_cnt = a.find(class_='property').find(class_='look').strong.text.strip()
        except:
            view_cnt = ''
            print 'view count time not found', domain_url
          
        try:
            summary = a.find(id='contenttxt').text.strip()
        except:
            summary = ''
            print 'summary time not found', domain_url

        try:
            body = '\n'.join([txt.text for txt in  a.find(id='contenttxt').find_all('p')])
        except:
            body = ''
            print 'body time not found', domain_url


        comment_cnt = ''
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]


        
    if source == u'飞象网':
        title = a.title.text.split(' ')[0]#[sib for sib in a.find(class_='dateAndSource').parent.parent.parent.parent.previous_siblings][-2].text.strip()
        
        tmp = a.find(string=re.compile("^20\d\d" + u"年" + "[0-9]*" + u"月"))
        str_tmp = tmp.parent.text.encode('utf-8')
        if '：' in str_tmp:
            author = str_tmp.split('：')[-1]
        else:
            author = ''
        
        #author = a.find(class_='dateAndSource').text.split(':')[-1].strip()
        try:
            publish_time = time_process_func(str_tmp.split(' ')[0])
        except Exception,e:
            print Exception,e
            print a.find(class_='dateAndSource').text.split(' ')[0]
        view_cnt = ''
        summary = ''
        body = '\n'.join([txt.text for txt in a.find(class_='art_content').find_all('p')])
        comment_cnt = ''
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]


        
    if source == u'每日科技网':
        title = a.h1.text.strip()
        author = ''
        publish_time = ' '.join(a.find(id='artical_sth').p.text.strip().split(' ')[:2])
        view_cnt = ''
        summary = ''
        body = a.find(id='main_content').text.strip() 
        comment_cnt = ''        
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]


        
    if source == u'E安全':
        title = a.h1.text
        author = ''
        publish_time = a.find(class_='lab').find(href=re.compile('javascript*')).text.strip() + ':00'
        view_cnt = ''
        summary = ''
        body = '\n'.join([txt.text.strip() for txt in a.find(class_='content-text').find_all('p')])
        comment_cnt = ''
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]


        
    if source == u'91科技':
        title = a.find(class_='arc-tit').text
        author = ''
        publish_time = a.find(class_='post-info').find(class_='time').text.encode('utf-8').split('：')[-1]
        view_cnt = ''
        summary = a.find(class_='arc-body-info').text.strip()
        body = '\n'.join([txt.text.strip() for txt in a.find(class_='arc-body').find_all('p')])
        comment_cnt = ''
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]


        
    if source == u'启明星辰':
        title = a.find(id='InfoTitle').text
        author = a.find(id='InfoOther').text.encode('utf-8').split('：')[-1]
        publish_time = a.find(id='InfoOther').text.encode('utf-8').split(' ')[2].split('：')[1] + ' 00:00:00'
        view_cnt = ''
        summary = ''
        body = '\n'.join([txt.text.strip() for txt in a.find(id='ContentBodyShow').find_all('span')])
        comment_cnt = ''
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]


        
    if source == u'驱动中国':
        title = a.h1.text.strip()
        author = ''
        publish_time = time_process_func(a.find(id='pubtime').text.strip())
        view_cnt = ''
        summary = ''
        body = '\n'.join([txt.text.strip() for txt in a.find(class_='art_txt').find_all('p')])
        comment_cnt = ''
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]


        
    if source == u'网信办':
        title = a.title.text.split('_')[0]
        if a.title.text.split('_')[-1] == 'redirector':
            return None
        
        author = ''
        try:
            publish_time = time_process_func(a.find(id='pubtime').text.strip().encode('utf-8'))
        except Exception,e:
            publish_time = ''
            print 'publis_time not found', domain_url
        view_cnt = ''
        summary = ''
        body = a.find(id='content').text.strip()
        comment_cnt = ''
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]


        
    if source == u'红黑联盟':
        try:
            title = a.find(class_='box_left').find(class_='box_t').text
        except:
            try:
                title = a.find(class_='box_left').find('h1').text
            except:
                title = ''
                
        author = ''
        publish_time = a.find(class_='box_left').find(class_='frinfo').text.strip().split(' ')[0] + ' 00:00:00'
        view_cnt = ''
        summary = ''
        body = '\n'.join([txt.text.strip() for txt in a.find(id='Article').find_all('p')])  
        comment_cnt = ''
        
        return [title, author, publish_time, view_cnt, summary, body, comment_cnt]
    

    return None
