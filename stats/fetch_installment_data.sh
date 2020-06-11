#!/bin/bash

fr=$1
to=$2

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.adhoc;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

create temporary table ronghui.rlab_stats_report_installment_raw (imei String, app_package String, app_name String, status int)
partitioned by(data_date String)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS ORC;

insert overwrite table ronghui.rlab_stats_report_installment_raw
partition(data_date)
select
	md5(imei) device,
	package app_package,
	name app_name,
	status,
	data_date
from
	edw.app_list_install_uninstall_fact
where
	data_date between '$1' and '$2';

create table if not exist ronghui.rlab_stats_report_installment_overall (uninstall_record_count int, newly_install_record_count int)
partitioned(data_date String)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS ORC;

insert overwrite table ronghui.rlab_stats_report_installment_overall
partition(data_date)
select
	data_date,
	sum(case when status = 0 then 1 else 0 end) uninstall_record_count,
	sum(case when status = 2 then 1 else 0 end) newly_install_record_count
from
	ronghui.rlab_stats_report_installment_raw
group by
	data_date;

create table if not exist ronghui.rlab_stats_report_installment_device (device String, status int, device_record_count int, device_app_count int)
partitioned by(data_date String)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS ORC;

insert overwrite table ronghui.rlab_stats_report_installment_device
partition(data_date)
select
	device,
	status,
	count(1) device_record_count,
	count(distinct app_package) device_app_count,
	data_date
from
	ronghui.rlab_stats_report_installment_raw
group by
	device,
	status,
	data_date

create table if not exist ronghui.rlab_stats_report_installment_app (app_package String, status int, app_record_count int, app_device_count int)
partitioned by(data_date String)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS ORC;

insert overwrite table ronghui.rlab_stats_report_installment_app
partition(data_date)
select
    app_package,
    status,
    count(1) app_record_count,
    count(distinct device) app_device_count,
    data_date
from
    ronghui.rlab_stats_report_installment_raw
group by
    app,
    status,
    data_date
"

nohup beeline -e "$job" &