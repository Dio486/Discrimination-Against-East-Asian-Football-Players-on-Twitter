# coding=utf-8
import datetime
import re

import pandas as pd
import time
from urllib import parse
import traceback
import pymysql
import requests
import jsonpath
from dateutil import parser as date_parser
import gc

# 添加代理，在国内访问twitter需要挂境外代理
proxies =  {'https':''}
# 查詢參數，數量，偏移量
API = ""
def parseTweets(tweets,users,target_time,start_time,end_time):
    target_Ts = datetime.datetime.strptime(target_time, '%Y-%m-%d %H:%M:%S')
    endTs = datetime.datetime.strptime(end_time, '%Y-%m-%d')
    startTs = datetime.datetime.strptime(start_time, '%Y-%m-%d')
    for tweet in tweets:
        try:
            tweet = tweet.get('legacy')
            parse = date_parser.parse(tweet.get('created_at'))
            created_at = (parse + datetime.timedelta(hours=0)).strftime("%Y-%m-%d %H:%M:%S")

            createdTs = datetime.datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')

            if createdTs > endTs:
                continue
            if createdTs < startTs:
                return None

            content = tweet.get('full_text')
            user_id = tweet.get('user_id_str')
            extended_entities = tweet.get('extended_entities')
            reply_count = tweet.get('reply_count')
            favorite_count = tweet.get('favorite_count')
            retweet_count = tweet.get('retweet_count')
            article_id = tweet.get('id_str')
            screen_name = 'shenzhen'
            user_location = ''
            photo = ''
            name = ''
            video = ''
            if extended_entities != None:
                photo = parsePhoto(extended_entities)
                video= parseVideo(extended_entities)
            for user in users:
                # 用户信息全在这里面，这里只解析了名称，需要其他信息可以从里面抽取
                if(user.get('rest_id')==user_id):
                    user = user.get('legacy')
                    name = user.get('name')
                    screen_name = user.get('screen_name')
                    user_location = user.get('location')
            tweetUrl = "https://twitter.com/{}/status/{}".format(screen_name, article_id)
            dataSet.append({
                'created_at':created_at,
                'content':content,'reply_count':reply_count,'favorite_count':favorite_count,'retweet_count':retweet_count,'article_id':article_id,
                'photo':photo,'name':name,'tweetUrl':tweetUrl,'video':video,'user_location':user_location})

        except Exception as e:
            print("parse tweets error!!")

    print("crawler size:{}.".format(len(dataSet)))

    return True


def parsePhoto(extended_entities):
    img_url = []
    for extended_entity in extended_entities.get('media'):
        if extended_entity.get('type') == 'photo':
            img_url.append(extended_entity.get('media_url_https'))
            return ",".join(str(i) for i in img_url)

def parseVideo(extended_entities):
    img_url = []
    for extended_entity in extended_entities.get('media'):
        get = extended_entity.get('type')
        if get == 'video':
            try:
                url_ = extended_entity.get('video_info')['variants'][0]['url']
                img_url.append(url_)
            except Exception as e:
                pass
            return ",".join(str(i) for i in img_url)

def parseCursor(json_data):
    values = jsonpath.jsonpath(json_data, '$..content')
    for value in values:
        if value.get("cursorType")=="Bottom":
            return value.get("value")

    return None

def export_excel(export, excelPath):
   pf = pd.DataFrame(export)
   order = ['created_at','content','reply_count','favorite_count','retweet_count','article_id','photo','name','tweetUrl','video','user_location']
   pf = pf[order]
   columns_map = {
      'created_at':'created_at',
      'content':'content',
      'reply_count':'reply_count',
       'favorite_count':'favorite_count',
       'retweet_count':'retweet_count',
       'article_id':'article_id',
       'photo':'photo',
       'name':'name',
       'tweetUrl':'tweetUrl',
       'video':'video',
       'user_location':'user_location'
   }
   pf.rename(columns = columns_map,inplace = True)
   file_path = excelPath
   pf.fillna(' ',inplace = True)
   pf.to_excel(file_path,encoding = 'utf-8',index = False)
   # file_path.save()

# 程序入口
if __name__ == '__main__':
    accounts = pd.read_excel('accounts.xlsx',engine='openpyxl')
    acc = 0
    # 这里更换成自己帐号的cookie，x-csrf-token，authorization
    total_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'cookie': str(accounts.loc[acc,'cookie']),
        'x-csrf-token': str(accounts.loc[acc,'x-csrf-token']),
        'authorization':str(accounts.loc[acc,'authorization'])
    }
    targets = pd.read_excel('parameters_single.xlsx',engine='openpyxl')
    targets.fillna('',inplace=True)
    for k in targets.index:
        dataSet = []
        code_start = datetime.datetime.now()
        target_time = str(targets.loc[k, 'time'])
        content = str(targets.loc[k, 'text'])
        path = str(targets.loc[k, 'path'])
        print(target_time)
        print(content)

        start_time = str(datetime.datetime.strptime(target_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours = -1))[:10]
        end_time = str(datetime.datetime.strptime(target_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours = 60))[:10]

        kw = '{} lang:en until:{} since:{}'.format(content, end_time, start_time)
        # cursor 初始化为空即可
        # cursor = ''
        cursor = str(targets.loc[k, 'token'])
        i = 0
        pri = 0
        while True: #每次翻页
            try:
                # 打印的cursor值是关键翻页参数，后面翻页靠的就是他
                print(cursor)
                request_api = API.format(parse.quote(kw),50, parse.quote(cursor))
                json_data = requests.get(url = request_api, headers = total_headers, proxies = proxies).json()
                tweets = jsonpath.jsonpath(json_data, "$..tweet_results.result")
                if tweets is False:
                    print('is_tweets_false:')
                    print(tweets)
                    break
                users = jsonpath.jsonpath(json_data, "$..user_results.result")
                tweets = parseTweets(tweets, users,target_time,start_time,end_time)
                if tweets is False:
                    print(tweets)
                    break
                cursor = parseCursor(json_data)
                time.sleep(5)
                if cursor is None or cursor == '':
                    print('cursor is empty! crawler over.')
                    break
                i = 0
                print('data come to {} now'.format(dataSet[-1]['created_at']))
                pri = pri + 20
                if pri >= 1000:
                    pri = 0
                    file_name = './data/final/twitter_{}_{}.xlsx'.format(acc,path)
                    export_excel(dataSet, file_name)
            except:
                print(cursor)

                time.sleep(30)
                i = i + 1
                print('request : '+ str(i))
                print('keep running {} now'.format(datetime.datetime.now() - code_start))
                if i >=120:
                    break
                continue
        dataSetByLocation = []
        file_name = './data/final/twitter_{}_{}.xlsx'.format(acc,path)
        if dataSet != []:
            export_excel(dataSet, file_name)
        for cell in dataSet:
            get = cell.get('user_location')
        code_end = datetime.datetime.now()
        print(code_end - code_start)
        targets.loc[k,'token'] = cursor
        targets.to_excel('parameters.xlsx',index=False)
        print('cursor save successfully!!!!')
    print('----------------------all_match is over!!---------------------------')



