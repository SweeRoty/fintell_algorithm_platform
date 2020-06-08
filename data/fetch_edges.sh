#!/bin/bash

date=$1

job="
use ronghui_mart;
set mapreduce.job.queuename=root.ronghui.adhoc;

insert overwrite directory './hgy/sample'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
select
	uid,
	stat_uid_act_cnt,
	stat_uid_usetime_sum
from
	ronghui_mart.rh_base_user_01_uid
where
	data_date='$1' 
limit 5
"

nohup beeline --showHeader=true -e "$job" > log_tmp &