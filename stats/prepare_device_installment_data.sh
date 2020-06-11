#!/bin/bash

job="
use ronghui_mart;
set mapreduce.job.queuename=root.ronghui.adhoc;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

create table ronghui.rlab_stats_report_installment_device (imei String, status int, device_record_count int, device_app_count int)
partitioned by(year String, month String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table ronghui.rlab_stats_report_installment_device
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
select
	imei,
	status,
	count(1) device_record_count,
	count(distinct app) device_app_count,
	year,
	month
from
	ronghui.rlab_stats_report_installment_raw
group by
	imei,
	status,
	year,
	month
"

nohup beeline -e "$job" &