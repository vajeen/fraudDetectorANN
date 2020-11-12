#!/usr/bin/python3
import os
import sys
import subprocess
import numpy as np
from multiprocessing import Process, Queue, Lock, Value
from pymongo import MongoClient
import time
import MySQLdb
import threading

hour=sys.argv[2]
dd=sys.argv[1]
month=dd.split('-')[1]
day=dd.split('-')[2]
tbldd="{0}{1}".format(month,day)

        
def preprocess(q,i,hour,day):
    client = MongoClient('mongodb://root:1cbtm0ngo@172.19.151.56:27017/')
    db = getattr(client, "HOURLY_{0}".format(tbldd))
    col = getattr(db, "M{0}".format(hour))
    
    mydb = MySQLdb.connect("172.19.6.232","root","icbt123","LEARN")
    cursor = mydb.cursor()
    
    while True:
        num = q.get()
        
        if "KILL" in num:
            break
        else:
            result = col.find({"MSISDN": "{0}".format(num[0])})
            #print(result)
            for d in result:
                #print(d)
                if 'MO_SMS_B_NUMBER_UNIQUE' in d:
                    mo_sms_count = len(d['MO_SMS_B_NUMBER_UNIQUE'])
                else:
                    mo_sms_count = 0
            
                if 'MO_SMS_LC_UNIQUE' in d:
                    mo_sms_lac = len(d['MO_SMS_LC_UNIQUE'])
                else:
                    mo_sms_lac = 0
                    
                if 'MT_SMS_A_NUMBER_UNIQUE' in d:
                    mt_sms_count = len(d['MT_SMS_A_NUMBER_UNIQUE'])
                else:
                    mt_sms_count = 0
                    
                if 'MT_SMS_LC_UNIQUE' in d:
                    mt_sms_lac = len(d['MT_SMS_LC_UNIQUE'])
                else:
                    mt_sms_lac = 0
                    
                if 'MO_CALL_B_NUMBER_UNIQUE' in d:
                    mo_call_count = len(d['MO_CALL_B_NUMBER_UNIQUE'])
                else:
                    mo_call_count = 0
            
                if 'MO_CALL_LC_UNIQUE' in d:
                    mo_call_lac = len(d['MO_CALL_LC_UNIQUE'])
                else:
                    mo_call_lac = 0
            
                if 'MO_CALL_IMEI_UNIQUE' in d:
                    mo_call_imei = len(d['MO_CALL_IMEI_UNIQUE'])
                else:
                    mo_call_imei = 0
            
                if 'MO_CALL_DURATION' in d:
                    tmp = [int(i) for i in d['MO_CALL_DURATION']]
                    mo_call_time = sum(tmp)
                else:
                    mo_call_time = 0
            
                if 'MT_CALL_A_NUMBER_UNIQUE' in d:
                    mt_call_count = len(d['MT_CALL_A_NUMBER_UNIQUE'])
                else:
                    mt_call_count = 0
            
                if 'MT_CALL_LC_UNIQUE' in d:
                    mt_call_lac = len(d['MT_CALL_LC_UNIQUE'])
                else:
                    mt_call_lac = 0
            
                if 'MT_CALL_IMEI_UNIQUE' in d:
                    mt_call_imei = len(d['MT_CALL_IMEI_UNIQUE'])
                else:
                    mt_call_imei = 0
            
                if 'MT_CALL_DURATION' in d:
                    tmp = [int(i) for i in d['MT_CALL_DURATION']]
                    mt_call_time = sum(tmp)
                else:
                    mt_call_time = 0
                numlist = [int(d['MSISDN']),mo_sms_count,mo_sms_lac,mt_sms_count,mt_sms_lac,mo_call_count,mo_call_lac,mo_call_imei,mo_call_time,mt_call_count,mt_call_lac,mt_call_imei,mt_call_time]
                cursor.execute("INSERT INTO `LEARN`.`DATA_04` (`MSISDN`, `DAY`, `HOUR`, `MO_SMS_B_NUMBER_UNIQUE`, `MO_SMS_LC_UNIQUE`, `MT_SMS_A_NUMBER_UNIQUE`, `MT_SMS_LC_UNIQUE`, `MO_CALL_B_NUMBER_UNIQUE`, `MO_CALL_LC_UNIQUE`, `MO_CALL_IMEI_UNIQUE`, `MO_CALL_DURATION`, `MT_CALL_A_NUMBER_UNIQUE`, `MT_CALL_LC_UNIQUE`, `MT_CALL_IMEI_UNIQUE`, `MT_CALL_DURATION`,FRAUD) VALUES ({0},'{1}',{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15});".format(numlist[0],day,hour,numlist[1],numlist[2],numlist[3],numlist[4],numlist[5],numlist[6],numlist[7],numlist[8],numlist[9],numlist[10],numlist[11],numlist[12],num[1]))
                mydb.commit()
                del numlist[:]
    mydb.close()
    

if __name__ == '__main__':
    i = 0
    processers = 50
    q = Queue()
    processes = []
    for p in range(processers):
        #print("Spawning process, PROCESS: {0}".format(str(i)))
        p = Process(target=preprocess, args=(q,i,hour,dd))
        p.start()
        processes.append(p)
        i = i + 1
        #time.sleep(.1)
    with open('/apps/ANN/production/confirmed2.txt') as fp:
        fraud = [int(line.rstrip('\n')) for line in fp]
    for num in fraud:
        number = "0{0}".format(num)
        q.put([number,1])
    
    with open('/apps/ANN/production/normal.txt') as fp:
        normal = [int(line.rstrip('\n')) for line in fp]
    for num in normal:
        number = "0{0}".format(num)
        q.put([number,0])
        
    for pp in range(processers):
        q.put(["KILL"])
    
    for process in processes:
        process.join()
    #print("All Done!!!")