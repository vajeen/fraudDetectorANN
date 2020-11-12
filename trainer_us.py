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

mydb = MySQLdb.connect("172.19.6.232","root","icbt123","LEARN")
cursor = mydb.cursor()

cursor.execute("select `MO_SMS_B_NUMBER_UNIQUE`, `MO_SMS_LC_UNIQUE`, `MT_SMS_A_NUMBER_UNIQUE`, `MT_SMS_LC_UNIQUE`, `MO_CALL_B_NUMBER_UNIQUE`, `MO_CALL_LC_UNIQUE`, `MO_CALL_IMEI_UNIQUE`, `MO_CALL_DURATION`, `MT_CALL_A_NUMBER_UNIQUE`, `MT_CALL_LC_UNIQUE`, `MT_CALL_IMEI_UNIQUE`, `MT_CALL_DURATION` from `DATA_04_Filtered` where fraud=1 and MO_CALL_B_NUMBER_UNIQUE>5")
data1 = cursor.fetchall()
res1 = [1] * len(data1)

cursor.execute("select `MO_SMS_B_NUMBER_UNIQUE`, `MO_SMS_LC_UNIQUE`, `MT_SMS_A_NUMBER_UNIQUE`, `MT_SMS_LC_UNIQUE`, `MO_CALL_B_NUMBER_UNIQUE`, `MO_CALL_LC_UNIQUE`, `MO_CALL_IMEI_UNIQUE`, `MO_CALL_DURATION`, `MT_CALL_A_NUMBER_UNIQUE`, `MT_CALL_LC_UNIQUE`, `MT_CALL_IMEI_UNIQUE`, `MT_CALL_DURATION` from `DATA_04_Filtered` where fraud=0 limit 5000")
data2 = cursor.fetchall()
res2 = [0] * len(data2)

train_mat = np.matrix(data1+data2)
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
model.fit(train_mat, res_mat, epochs=200, batch_size=8)
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