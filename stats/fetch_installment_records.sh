#!/bin/bash

fr=$1
to=$2

job="
set mapreduce.job.queuename=root.ronghui.adhoc;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

create table ronghui.rlab_stats_report_installment_raw (imei String, app String, status int)
partitioned by(year String, month String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table ronghui.rlab_stats_report_installment_raw
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
select
	imei,
	concat(package, name) app,
	status,
	substring(data_date, 1, 4) year,
	substring(data_date, 5, 2) month
from
	edw.app_list_install_uninstall_fact
where
	data_date between '$1' and '$2'
"

nohup beeline -e "$job" &