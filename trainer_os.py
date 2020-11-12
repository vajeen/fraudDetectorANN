#!/usr/bin/python3
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import numpy as np
from keras.models import load_model
from numpy import asarray
from numpy import save
import MySQLdb
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, roc_curve, auc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sn
from random import randrange

mydb = MySQLdb.connect("172.19.6.232","root","icbt123","LEARN")
cursor = mydb.cursor()

cursor.execute("select `MO_SMS_B_NUMBER_UNIQUE`, `MO_SMS_LC_UNIQUE`, `MT_SMS_A_NUMBER_UNIQUE`, `MT_SMS_LC_UNIQUE`, `MO_CALL_B_NUMBER_UNIQUE`, `MO_CALL_LC_UNIQUE`, `MO_CALL_IMEI_UNIQUE`, `MO_CALL_DURATION`, `MT_CALL_A_NUMBER_UNIQUE`, `MT_CALL_LC_UNIQUE`, `MT_CALL_IMEI_UNIQUE`, `MT_CALL_DURATION` from `DATA_04` where fraud=1 and MO_CALL_B_NUMBER_UNIQUE>10 and MT_CALL_A_NUMBER_UNIQUE<3")
data1 = cursor.fetchall()
res1 = [1] * len(data1)

cursor.execute("select `MO_SMS_B_NUMBER_UNIQUE`, `MO_SMS_LC_UNIQUE`, `MT_SMS_A_NUMBER_UNIQUE`, `MT_SMS_LC_UNIQUE`, `MO_CALL_B_NUMBER_UNIQUE`, `MO_CALL_LC_UNIQUE`, `MO_CALL_IMEI_UNIQUE`, `MO_CALL_DURATION`, `MT_CALL_A_NUMBER_UNIQUE`, `MT_CALL_LC_UNIQUE`, `MT_CALL_IMEI_UNIQUE`, `MT_CALL_DURATION` from `DATA_04` where fraud=0 limit 500000")
data2 = cursor.fetchall()
res2 = [0] * len(data2)

#Oversampling
os_count = int(len(data2) * 25/75)
cur_count = len(data1)
fraud1 = []
fraud2 = []

print(len(data1))
print(os_count)
print(cur_count)


for i in data1:
	fraud1.append([i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9],i[10],i[11]])

while True:
	for i in fraud1:
		#print(i)
		fraud2.append([i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7]+randrange(200),i[8],i[9],i[10],i[11]])
		cur_count+=1
		#print(cur_count)
		if cur_count >= os_count:
			break
	if cur_count >= os_count:
			break
fraud = fraud1+fraud2
res1 = [1] * len(fraud)
print(len(fraud))
for i in data2:
	fraud.append(i)
	
train_mat = np.matrix(fraud)
res_mat = np.matrix(res1+res2).transpose()

#Model1
model = Sequential()
model.add(Dense(12, input_dim=12, activation='relu'))
model.add(Dense(6, activation='relu'))
model.add(Dense(4, activation='relu'))
model.add(Dense(1, activation='sigmoid'))

#Model2
# model = Sequential()
# model.add(Dense(12, input_dim=12, activation='relu'))
# model.add(Dense(7, activation='relu'))
# model.add(Dense(1, activation='sigmoid'))

model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
model.fit(train_mat, res_mat, epochs=100, batch_size=32)
accuracy = model.evaluate(train_mat, res_mat)

pred = model.predict_classes(train_mat)
#pred = (pred > 0.5)
#score = model.evaluate(train_mat, res_mat)
#print(score)

print(classification_report(res_mat, pred))
#print('Accuracy: %.2f' % (accuracy*100))

cm = confusion_matrix(res_mat, pred) # rows = truth, cols = prediction
df_cm = pd.DataFrame(cm, index = (0, 1), columns = (0, 1))
plt.figure(figsize = (10,7))
sn.set(font_scale=1.4)
sn.heatmap(df_cm, annot=True, fmt='g')
print("Test Data Accuracy: %0.4f" % accuracy_score(res_mat, pred))
plt.savefig('confusion.png')

fpr, tpr, thresholds = roc_curve(res_mat, pred)
roc_auc = auc(fpr, tpr)
plt.title('Receiver Operating Characteristic')
plt.plot(fpr, tpr, label='AUC = %0.4f'% roc_auc)
plt.legend(loc='lower right')
plt.plot([0,1],[0,1],'r--')
plt.xlim([-0.001, 1])
plt.ylim([0, 1.001])
plt.ylabel('True Positive Rate')
plt.xlabel('False Positive Rate')
plt.savefig('roc.png')

print("Saving")
model.save('/apps/ANN/production/model.h5')
