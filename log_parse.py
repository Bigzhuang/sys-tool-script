# coding=utf-8
import arrow
import re
import os
import sys
import requests
import socket
from argparse import ArgumentParser

class data_merge():
    '''分析五分钟前的日志'''
    # db_url = 'http://101.201.69.176:8888/ops3/edge2db'

    def __init__(self, time=None, send_to_db=False, db_api=None, log_path=None):
        self.time=arrow.get(time, 'YYYY-MM-DD HH:mm') if time else arrow.now()
        self.log_time=self.time.replace(minutes=-5)
        self.timestamp = self.time.replace(second=0).timestamp
        self.datetime = self.time.replace(second=0).format('YYYY-MM-DD HH:mm:ss')
        self.date=self.log_time.format('YYYY_MM_DD')
        self.hour=self.log_time.format('HH')
        self.channel_data = {}
        self.hostname = socket.gethostname()
        self.send_to_db = send_to_db
        self.db_url = db_api
        self.log_path = log_path

    def retrive_data(self,file):
        '''parse access log from nginx minute log'''
        with open(file,'r')as f:
            for line in f.readlines():
                if not re.match('\d{10}.*',line):#过滤汇添富日志
                    continue
                splited_line = re.split('\s',line)
                channel_id = splited_line[-2]
                if channel_id == '-':
                    continue
                flow = int(splited_line[-4]) #byte
                flow_type = splited_line[-6].split(':')[0] #MISS|HIT|PASS
                if channel_id not in self.channel_data:
                    self.channel_data[channel_id] = {flow_type:[flow]}
                elif flow_type not in self.channel_data[channel_id]:
                    self.channel_data[channel_id][flow_type]=[flow]
                else:
                    self.channel_data[channel_id][flow_type].append(flow) #{cid:{'HIT':['12','12'],'MISS':['11','14']}
        return self.channel_data

    def format_channel_data(self):
        '''重新格式化channel_data'''
        for cid in self.channel_data:
            for flow_type,flows in self.channel_data[cid].items():
                self.channel_data[cid][flow_type]={'times':len(flows),'sum':sum(flows)}

    def merge2all(self):
        '''将全部数据合并为all频道'''
        hit_times,hit_sum,miss_times,miss_sum,pass_times,pass_sum = 0,0,0,0,0,0
        for cid,type_data in self.channel_data.items():
            if type_data.get('HIT'):
                hit_sum += type_data['HIT']['sum']
                hit_times += type_data['HIT']['times']
            if type_data.get('PASS'):
                pass_times += type_data['PASS']['times']
                pass_sum += type_data['PASS']['sum']
            if type_data.get('MISS'):
                miss_times += type_data['MISS']['times']
                miss_sum += type_data['MISS']['sum']
        self.channel_data['all'] = {
                'HIT':{'sum':hit_sum,'times':hit_times},
                'PASS':{'sum':pass_sum,'times':pass_times},
                'MISS':{'sum':miss_sum,'times':miss_times},
                }

    @property
    def file_list(self):
        file_list=[]
        for i in range(5):
            file = '%s.log'%self.log_time.replace(minutes=i).format('mm')
            file_list.append(file)
        return file_list

    def send_to_db(self):
        query_data={
            'timestamp':str(self.timestamp),
            'datetime':self.datetime,
            'hostname':self.hostname,
            }
        post_data = {
            'channel_data':self.channel_data
        }
        respon = requests.post(self.db_url,json=post_data,params=query_data)
        return respon


    def main(self):
        os.chdir(self.log_path)
        if not os.path.exists(self.date):
            return 'file %s dont\'t exist'%self.date
        os.chdir(self.date)
        if not os.path.exists(self.hour):
            return 'file %s don\'t exist'%self.date
        os.chdir(self.hour)
        for file in self.file_list:
            if not os.path.exists(file):
                self.file_list.pop(file)
                continue
            self.retrive_data(file)
        self.format_channel_data()
        self.merge2all()
        if self.send_to_db:
            db_respon = self.send_to_db()
        return db_respon

if __name__ == '__main__':
    parse = ArgumentParser()
    parse.add_argument('url',help='ops node2db api format:"http://example.com"', type=str)
    parse.add_argument('--starttime', help='log recover start time format:"1990-12-24 11:05"')
    m=data_merge()
    print "time:%s timestamp:%s"%(m.datetime,m.timestamp)
    print "hour_file:%s  date_file:%s"%(m.hour,m.date)
    print "db response:%s"%m.main()
    print "file_list:",m.file_list
    for cid,data_type in m.channel_data.items():
        print cid,data_type