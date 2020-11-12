#!/usr/bin/python3
import os
import sys
import subprocess
import numpy as np
from multiprocessing import Process, Queue, Lock, Value
from pymongo import MongoClient
import keras
import h5py
import tensorflow as tf
import time
import MySQLdb
import threading

hour=sys.argv[2]
dd=sys.argv[1]
month=dd.split('-')[1]
day=dd.split('-')[2]
tbldd="{0}{1}".format(month,day)

client = MongoClient('mongodb://root:1cbtm0ngo@172.19.151.56:27017/')
ns = getattr(client, "HOURLY_{0}".format(tbldd))
db=getattr(ns, "M{0}".format(hour))
#db = client.HOURLY_0407.M10
cursor = db.find()
itersize = 10000
chunk_n=0
total_docs=0

class Killer(object):
    def __init__(self, initval=0):
        self.val = Value('i', initval)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value

def predict(q,q2,lock,i,kill,hour,day):
    mydb = MySQLdb.connect("172.19.6.232","root","icbt123","FRAUD")
    cursor = mydb.cursor()
    #data = cursor.fetchall()
    print("Initiating Predict")
    model = tf.keras.models.load_model('/apps/ANN/production/model.h5')
    fraud=0
    normal=0
    # summarize model.
    #model.summary()
    #lock.release()
    #time.sleep(10)
    while True:
        mat = q2.get()
        if isinstance(mat, np.matrix):
            try:
                predictions =  model.predict_classes(mat[:,1:])
            except Exception as e:
                print(e)
                print("mat ->")
                print(mat)
                sys.exit()
            unique, counts = np.unique(predictions, return_counts=True)
            b=dict(zip(unique, counts))
            if 1 in b:
                fraud+=b[1]
            if 0 in b:
                normal+=b[0]
            indexes = np.where(predictions == 1)[0]
            # for j in range(predictions.shape[0]):
            #     #some_function(j, theta[j], theta)
            #     print(predictions[j])
            #     print(mat[j][0].tolist()[0])
            #     cursor.execute("")
            
            for index in indexes:
                tmp_list=mat[index][0].tolist()[0]
                table="M{0}".format(str(tmp_list[0])[-2:])
                cursor.execute("INSERT INTO `FRAUD`.`{15}`(`MSISDN`, `DAY`, `HOUR`, `MO_SMS_B_NUMBER_UNIQUE`, `MO_SMS_LC_UNIQUE`, `MT_SMS_A_NUMBER_UNIQUE`, `MT_SMS_LC_UNIQUE`, `MO_CALL_B_NUMBER_UNIQUE`, `MO_CALL_LC_UNIQUE`, `MO_CALL_IMEI_UNIQUE`, `MO_CALL_DURATION`, `MT_CALL_A_NUMBER_UNIQUE`, `MT_CALL_LC_UNIQUE`, `MT_CALL_IMEI_UNIQUE`, `MT_CALL_DURATION`) VALUES ({0},'{1}',{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14});".format(tmp_list[0],day,hour,tmp_list[1],tmp_list[2],tmp_list[3],tmp_list[4],tmp_list[5],tmp_list[6],tmp_list[7],tmp_list[8],tmp_list[9],tmp_list[10],tmp_list[11],tmp_list[12],table))
                mydb.commit()
                
            #if predictions[0][0] == 1:
                #print([int(d['MSISDN']),mo_sms_count,mo_sms_lac,mt_sms_count,mt_sms_lac,mo_call_count,mo_call_lac,mo_call_imei,mo_call_time,mt_call_count,mt_call_lac,mt_call_imei,mt_call_time])
            #print(predictions)
        else:
            print("Prediction FINISHED!!!")
            print("Normal: {0}".format(normal))
            print("Fraud: {0}".format(fraud))
            cursor.execute("INSERT INTO `FRAUD`.`SUMMARY`(`DATE`, `HOUR`, `TOTAL`, `FRAUD`) VALUES ('{0}','{1}','{2}','{3}');".format(day,hour,fraud+normal,fraud))
            mydb.commit()
            mydb.close()
            break
        
def preprocess(q,q2,lock,i,kill):
    j=0
    data = []
    term = False
    while True:
        d = q.get()
        #print(d)
        if "KILL" in d:
            term = True
        else:
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
            thisdata = [int(d['MSISDN']),mo_sms_count,mo_sms_lac,mt_sms_count,mt_sms_lac,mo_call_count,mo_call_lac,mo_call_imei,mo_call_time,mt_call_count,mt_call_lac,mt_call_imei,mt_call_time]
            data.append(thisdata)
        #print("PROCESS{0} Q -> {1}, Size -> {3}, Kill Val -> {2}".format(i,q.empty(),kill.value(),q.qsize()))
        if term:
            #print("PROCESS {0} GOT KILL SIG".format(i))
            q2.put(np.matrix(data))
            #print("PROCESS {0} BREAK".format(i))
            break
        
        if j >= 9999:
            #print("PROCESS {0} PUT".format(i))
            q2.put(np.matrix(data))
            j=0
            del data[:]
        else:
            j+=1
        
    
def iterate_by_chunks(collection, chunksize=1, start_from=0, query={}):
	chunks = range(start_from, collection.find(query).count(), int(chunksize))
	num_chunks = len(chunks)
	for i in range(1,num_chunks+1):
		if i < num_chunks:
			yield collection.find(query)[chunks[i-1]:chunks[i]]
		else:
			yield collection.find(query)[chunks[i-1]:chunks.stop]

if __name__ == '__main__':
    kill = Killer(0)
    i = 0
    processers = 50 # Define number of data transformation processes
    q = Queue() # Initiate Main Queue that holds database records which the main thread will iterate and fetch
    q2 = Queue() # Initiate Queue that holds transformed matrixes for prediction
    lock = Lock() # Initiate lock
    processes = [] # processes bucket
    predict = Process(target=predict,args=(q,q2,lock,i,kill,hour,dd)) # Initiate prediction process
    predict.start()
    #Initiate transformation processes
    for p in range(processers):
        print("Spawning process, PROCESS: {0}".format(str(i)))
        p = Process(target=preprocess, args=(q,q2,lock,i,kill))
        p.start()
        processes.append(p)
        i = i + 1
        time.sleep(.1)
    
    #iterate and fetch db records
    mess_chunk_iter = iterate_by_chunks(db, itersize, 0, query={})
    for docs in mess_chunk_iter:
        chunk_n=chunk_n+1
        chunk_len = 0
        N=0
        for d in docs:
            q.put(d) #Queue 
    
    print("Finished reading. Sending Kill Sig")
    for pp in range(processers):
        q.put(["KILL"])
    while True:
        print("Q2Size: {0}".format(q2.qsize()))
        if q2.qsize() == 0:
            time.sleep(1)
            if q2.qsize() == 0:
                q2.put({"KILL":1})
                break
        time.sleep(1)
    print("Kill Sent")
    
    for process in processes:
        process.join()
    print("All Done!!!")