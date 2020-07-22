# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def loadSampledDevices(spark, data_date):
	sql = """
		select
			imei
		from
			ronghui.hgy_01
		where
			data_date = '{0}'
	""".format(data_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def retrieveRawRecords(spark, fr, to):
	sql = """
		select
			distinct md5(cast(imei as string)) imei,
			package app_package,
			status
		from
			edw.app_list_install_uninstall_fact
		where
			data_date between '{0}' and '{1}'
			and status != 1
			and from_unixtime(last_report_time, 'yyyyMMdd') = data_date
			and imei is not null
			and imei != ''
			and package is not null
			and package != ''
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
			.appName('RLab_Stats_Report___Prepare_APP_Installment_Stats') \
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
	##status = [0, 2]
	##result = {}
	records = retrieveRawRecords(spark, fr, to)
	#devices = spark.read.csv('/user/ronghui_safe/hgy/rlab_stats_report/sampled_devices/{0}'.format(args.query_month), header=True)
	devices = loadSampledDevices(spark, args.query_month)
	records = records.join(devices, on=['imei'], how='inner')
	apps = records.repartition(200, ['app_package']).groupBy(['app_package', 'status']).agg(\
		F.count(F.lit(1)).alias('app_installment_times'), \
		F.approx_count_distinct('imei', rsd=0.05).alias('app_installed_device_count'))
	##records.unpersist()
	apps.repartition(200).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installment/app_rank/{0}'.format(args.query_month), header=True)

	#result['new_installment_count'] = records.where(records.status == 2).count()
	#result['uninstallment_count'] = records.where(records.status == 0).count()
	'''
	for s in status:
		devices = records.repartition(1000, ['imei']).where(F.col('status') == s).groupBy(['imei']).agg(\
			F.count(F.lit(1)).alias('device_installment_times'), \
			F.approx_count_distinct('app_package', rsd=0.05).alias('device_installed_app_count'))
		devices_stats = devices.select(\
			F.count(F.lit(1)).alias('{0}_device_count'.format(s)), \
			F.mean('device_installment_times').alias('avg_installments_per_{0}_device'.format(s)), \
			F.mean('device_installed_app_count').alias('avg_installed_app_per_{0}_device'.format(s))).collect()
		print('=========>', devices_stats)
		result['{0}_device_count'.format(s)] = devices_stats[0]['{0}_device_count'.format(s)]
		result['avg_installments_per_{0}_device'.format(s)] = devices_stats[0]['avg_installments_per_{0}_device'.format(s)]
		result['avg_installed_app_per_{0}_device'.format(s)] = devices_stats[0]['avg_installed_app_per_{0}_device'.format(s)]

	apps = records.repartition(500, ['app_package']).groupBy(['app_package', 'status']).agg(\
		F.count(F.lit(1)).alias('app_installment_times'), \
		F.approx_count_distinct('imei', rsd=0.05).alias('app_installed_device_count')).cache()
	records.unpersist()
	apps.repartition(100).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installment/app_rank/{0}'.format(args.query_month), header=True)
	for s in status:
		apps_stats = apps.where(apps.status == s).select(\
			F.count(F.lit(1)).alias('{0}_app_count'.format(s)), \
			F.mean('app_installment_times').alias('avg_installments_per_{0}_app'.format(s)), \
			F.mean('app_installed_device_count').alias('avg_installed_device_per_{0}_app'.format(s))).collect()
		print('=========>', apps_stats)
		result['{0}_app_count'.format(s)] = apps_stats[0]['{0}_app_count'.format(s)]
		result['avg_installments_per_{0}_app'.format(s)] = apps_stats[0]['avg_installments_per_{0}_app'.format(s)]
		result['avg_installed_device_per_{0}_app'.format(s)] = apps_stats[0]['avg_installed_device_per_{0}_app'.format(s)]

	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installment/{0}'.format(args.query_month), header=True)
	'''