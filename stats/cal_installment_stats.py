# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def retrieveRawRecords(spark, fr, to):
	sql = """
		select
			md5(cast(imei as string)) imei,
			package app_package,
			status
		from
			edw.app_list_install_uninstall_fact
		where
			data_date between '{0}' and '{1}'
			and status != 1
			and from_unixtime(last_report_time, 'yyyyMMdd') = data_date
	""".format(fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

def transform_to_row(row_dict):
	global args
	row_dict['data_date'] = args.query_month
	return Row(**row_dict)

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('./config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Cal_Installment_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str)
	args = parser.parse_args()
	fr = args.query_month+'01'
	to = args.query_month+str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])

	print('====> Start calculation')
	status = [0, 2]
	result = {}
	records = retrieveRawRecords(spark, fr, to)
	devices = spark.read.csv('hgy/rlab_stats_report/active_devices/{0}/sampled_imei_list'.format(args.query_month), header=True)
	records = records.join(devices, on=['imei'], how='inner').cache()
	result['new_installment_count'] = records.where(records.status == 2).count()
	result['uninstallment_count'] = records.where(records.status == 0).count()

	devices = records.repartition(5000, ['imei']).groupBy(['imei', 'status']).agg(\
		F.count(F.lit(1)).alias('device_installment_times'), \
		F.approx_count_distinct('app_package', rsd=0.05).alias('device_installed_app_count')).cache()
	for s in status:
		devices_stats = None
		if s != 1:
			devices_stats = devices.where(devices.status == s).select(\
				F.count(F.lit(1)).alias('device_count_{0}'.format(s)), \
				F.mean('device_installment_times').alias('avg_installments_per_device_{0}'.format(s)), \
				F.mean('device_installed_app_count').alias('avg_installed_app_per_device_{0}'.format(s))).collect()
		else:
			devices_stats = devices.where(devices.status >= s).select(\
				F.count(F.lit(1)).alias('device_count_{0}'.format(s)), \
				F.mean('device_installment_times').alias('avg_installments_per_device_{0}'.format(s)), \
				F.mean('device_installed_app_count').alias('avg_installed_app_per_device_{0}'.format(s))).collect()
		print('=========>', devices_stats)
		result['device_count_{0}'.format(s)] = devices_stats[0]['device_count_{0}'.format(s)]
		result['avg_installments_per_device_{0}'.format(s)] = devices_stats[0]['avg_installments_per_device_{0}'.format(s)]
		result['avg_installed_app_per_device_{0}'.format(s)] = devices_stats[0]['avg_installed_app_per_device_{0}'.format(s)]
	devices.unpersist()

	apps = records.repartition(1000, ['app_package']).groupBy(['app_package', 'status']).agg(\
		F.count(F.lit(1)).alias('app_installment_times'), \
		F.approx_count_distinct('imei', rsd=0.05).alias('app_installed_device_count')).cache()
	for s in status:
		apps_stats = None
		if s != 1:
			apps_stats = apps.where(apps.status == s).select(\
				F.count(F.lit(1)).alias('app_count_{0}'.format(s)), \
				F.mean('app_installment_times').alias('avg_installments_per_app_{0}'.format(s)), \
				F.mean('app_installed_device_count').alias('avg_installed_device_per_app_{0}'.format(s))).collect()
		else:
			apps_stats = apps.where(apps.status >= s).select(\
				F.count(F.lit(1)).alias('app_count_{0}'.format(s)), \
				F.mean('app_installment_times').alias('avg_installments_per_app_{0}'.format(s)), \
				F.mean('app_installed_device_count').alias('avg_installed_device_per_app_{0}'.format(s))).collect()
		print('=========>', apps_stats)
		result['app_count_{0}'.format(s)] = apps_stats[0]['app_count_{0}'.format(s)]
		result['avg_installments_per_app_{0}'.format(s)] = apps_stats[0]['avg_installments_per_app_{0}'.format(s)]
		result['avg_installed_device_per_app_{0}'.format(s)] = apps_stats[0]['avg_installed_device_per_app_{0}'.format(s)]
	apps.unpersist()

	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installment/{0}'.format(args.query_month), header=True)