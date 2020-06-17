#!/bin/bash

query_months=(202001 202002 202003 202004 202005)
for i in ${!query_months[@]}; 
do
  nohup spark-submit extract_active_devices.py --query_month ${query_months[$i]} &
  nohup spark-submit prepare_device_info.py --query_month ${query_months[$i]} &
done
nohup spark-submit prepare_app_category.py &