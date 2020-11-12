# fraudDetectorANN
Contains files related to Fraud Detector Artificial Neural Network Modules

## hourly_prediction.py
This is the hourly prediction module which uses trained network from model.h5
Collects input from MongoDB respective hourly collection
Outputs to mysql hundred divided tables

## hourly_scheduler.sh
Automation script for hourly_prediction.py which executed tipically by a hourly cron job

## model.h5
Saved neural network

## save_to_db_select.py
Collect sampling data from MongoDB and store the pre processed records in MySQL table for easy retreaval.
Used for training the network.

## trainer_os.py
Training module with over sampling approach
Input - MySQL Table (Run save_to_db_select.py)
Output - file: model.h5

## trainer_us.py
Training module with under sampling approach
Input - MySQL Table (Run save_to_db_select.py)
Output - file: model.h5