#!/bin/bash

date=$1

job="
use ronghui_mart;
set mapreduce.job.queuename=root.ronghui.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

insert overwrite directory './hgy/samples'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
select
	user.uid uid,
	user.package_name app_package_name,
	user.imei imei
from
(select
	uid,
	package_name,
	imei
from
	ronghui.register_user_log
where
	data_date='$1'
	and platform='a'
) user
left outer join
(select
	distinct imei,
	shanzhai_flag sz_flag
from
	ronghui_mart.sz_device_list
where
	data_date='20200531'
) sz
on
	user.imei = sz.imei
where
	sz.sz_flag is null
"

nohup beeline -e "$job" &