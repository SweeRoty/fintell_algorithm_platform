#!/bin/bash

fr=$1
to=$2
data_date=`echo $fr | cut -c1-6`

job="
use ronghui;
set mapreduce.job.queuename=root.partner.ronghui.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;
set hive.exec.reducers.max=20000;
set mapreduce.job.name=RLab_installment_for_app;

insert overwrite table ronghui.hgy_03
partition
(
	data_date='$data_date'
)
select
	joined_data.app_package app_package,
	joined_data.status status,
	count(1) record_times,
	count(distinct joined_data.imei) device_count
from
	(select
		samples.imei imei,
		installments.app_package app_package,
		installments.status status
	from
		(select
			imei
		from
			ronghui.hgy_01
		where
			data_date = '$fr'
		) samples
		join
		(select
			distinct md5(cast(imei as string)) imei,
			package app_package,
			status
		from
			ronghui.mx_ori_app_list_fact
		where
			data_date between '$fr' and '$to'
			and status != 1
			and from_unixtime(cast(last_report_time as int), 'yyyyMMdd') = data_date
			and imei is not null
			and imei != ''
			and package is not null
			and package != ''
		) installments
		on
			samples.imei = installments.imei) joined_data
group by
	joined_data.app_package, joined_data.status
"

nohup beeline -e "$job" > log_installment_app &