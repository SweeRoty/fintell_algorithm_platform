#!/bin/bash

query_months=(202001 202002 202003 202004 202005)
for i in ${!query_months[@]}; 
do
  nohup spark-submit cal_active_stats.py --query_month ${query_months[$i]} --os a &
  nohup spark-submit cal_online_stats.py --query_month ${query_months[$i]} --os a &
done