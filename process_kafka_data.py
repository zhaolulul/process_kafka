#encoding=utf8
from kafka import KafkaConsumer
import json 
import pymysql
import logging
import requests
from typing import Dict, List
from pathlib import Path
import numpy as np  
remote_url='http://119.3.63.235:80/investmentroadmap'
root_dir = Path(__file__).resolve().parents[1]
logging.basicConfig(
   format="%(asctime)s - %(levelname)s - %(name)s - PID: %(process)d -  %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
#写个方法/类 
CONNECT_CONFIG={
       'host': '192.168.11.89',
        'port': 3306,
        'user': 'root',
        'passwd': 'password',
        'charset':'utf8mb4',
        'cursorclass':pymysql.cursors.DictCursor
    }

conn = pymysql.connect(**CONNECT_CONFIG,autocommit=True)
cursor = conn.cursor() 
cursor.execute("SET NAMES utf8")
cursor.execute("SET CHARACTER_SET_CLIENT=utf8")
cursor.execute("SET CHARACTER_SET_RESULTS=utf8")
conn.commit()
def insert_mysql(values):
   logger.info("insert data")
 
   try:
      # 创建数据库
      db_name = 'investment1'
      cursor.execute('CREATE DATABASE IF NOT EXISTS %s' %db_name)
      conn.select_db(db_name)
   
      #创建表
      TABLE_NAME = 'investment_article'
      cursor.execute('CREATE TABLE if not EXISTS %s ( id varchar(256) NOT NULL,url varchar(255) DEFAULT NULL,title varchar(255) DEFAULT NULL,content longtext DEFAULT NULL,status varchar(256) DEFAULT NULL,keywords longtext DEFAULT NULL,top1 longtext DEFAULT NULL,top2 longtext DEFAULT NULL,top3 longtext DEFAULT NULL,jsondata longtext DEFAULT NULL, publishTime longtext DEFAULT NULL,comprehensiveFlag longtext DEFAULT NULL,positive longtext DEFAULT NULL,negative longtext DEFAULT NULL,error longtext DEFAULT NULL ) ENGINE=MyISAM DEFAULT CHARSET=utf8;' %TABLE_NAME)
      
      cursor.executemany('INSERT INTO investment_article values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")',values)

      #cursor.executemany('INSERT INTO investstock_related values("%s",%s,%s,%s,%s)',values)
     # 查询数据条目
      #count = cursor.execute('SELECT * FROM %s' %TABLE_NAME)
      #logger.info('total records:', cursor.rowcount)
      
      

   except Exception as result:
      # 发生错误时会滚
      conn.rollback()
      logger.error(f'{result}')
def connect_remote(title,content,url,autoTags,showTags):
   data={'title':title,"content":content,"autoTags":autoTags,"showTags":showTags}
   np.random.shuffle(headers)
   res=requests.post(url,data=json.dumps(data),headers=headers[0])
   return res   
def read(file: Path, mode: str = 'rb', encoding: str = 'utf-8') -> str:
   # check if file exists
   if not file.exists():
      print(f'File does not exist: {file}')

   if 'b' in mode:
      with open(file, mode=mode) as f:
         return f.read()
   else:
      with open(file, mode=mode, encoding=encoding) as f:
         return f.read()
def get_headers(file) -> List[Dict[str, str]]:
   user_agents = read(file).splitlines()
   return [{'user-agent': user_agent.strip(),'content-type':"application/json;charset=UTF-8"} for user_agent in user_agents]
headers = get_headers(root_dir / 'user-agents.txt')
from datetime import datetime 
haha=set()
def connect_kafka():
   consumer = KafkaConsumer('fhl-news-tag', group_id='investment',bootstrap_servers=["zk1.deepq.tech:9092", "zk2.deepq.tech:9092", "zk3.deepq.tech:9092"],max_poll_records=2000,heartbeat_interval_ms=20000)
   values=[]
   index=87800
   try:
      for msg in consumer:
         index+=1 
         json_obj=json.loads(msg.value)
         
         if json_obj["supplier"]=="小财智讯" or json_obj["source"]=="小财智讯":
            continue
         else:
            url=json_obj['url']
            title=json_obj['title']
            content=json_obj['content']
            # logger.info(f"autoTags{json_obj.get('autoTags')}")
            # logger.info(f"showTags{json_obj.get('showTags')}")
            try:
               res=connect_remote(title,content,remote_url,json_obj['autoTags'],json_obj['showTags'])
               if res.status_code==200:
                     res=res.json()
               else:
                  logger.error(f'error url:{url}')
                  continue
      
               new_data=[str(index),url,title,content]
               new_data.append( res['status'] if 'status' in res else '')
               new_data.append( res['keywords'] if 'keywords' in res else '')
               new_data.append( "\t".join(res['top1']) if 'top1' in res else '')
               new_data.append( "\t".join(res['top2']) if 'top2' in res else '')
               new_data.append( "\t".join(res['top3']) if 'top3' in res else '')
         
               new_data.append(json.dumps(res))
               new_data.append(datetime.now().strftime("%Y:%m:%d %H:%M:%S"))
               
               new_data.append(res['comprehensiveFlag']  if "comprehensiveFlag" in res else '')
               new_data.append(False)
               new_data.append(False)
               new_data.append("[]")
               values.append(tuple(new_data))
               if len(values)%10==0:
                  insert_mysql(values)
                  values=[]
                  logger.info(f"insert data num {index+1}")
            except Exception as result:
               logger.error(result) 
   except Exception as result:
      logger.error(result) 

         
      
      
if __name__=="__main__":
   connect_kafka()
     