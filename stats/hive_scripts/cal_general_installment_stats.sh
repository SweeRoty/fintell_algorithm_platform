#!/bin/bash

fr=$1
to=$2
status=$3
data_date=`echo $fr | cut -c1-6`

job="
use ronghui;
set mapreduce.job.queuename=root.partner.ronghui.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;
set hive.exec.reducers.max=20000;
set mapreduce.job.name=RLab_cal_general_installment_stats;

insert into table ronghui.hgy_04
partition
(
	data_date='$data_date'
)
select
	count(distinct md5(cast(imei as string))) device_count,
	count(distinct package) app_count,
	$status status
from
	edw.app_list_install_uninstall_fact
where
	data_date between '$fr' and '$to'
	and status = $status
	and from_unixtime(cast(last_report_time as int), 'yyyyMMdd') = data_date
	and imei is not null
	and imei != ''
	and package is not null
	and package != ''
"

nohup beeline -e "$job" &