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
			distinct md5(cast(imei as string)) imei,
			package app_package,
			data_date
		from
			edw.app_list_install_uninstall_fact
		where
			data_date between '{0}' and '{1}'
			and status >= 1
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
			.appName('RLab_Stats_Report___Cal_Installed_Stats') \
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
	result = {}
	records = retrieveRawRecords(spark, fr, to)
	devices = spark.read.csv('/user/ronghui_safe/hgy/rlab_stats_report/sampled_devices/{0}'.format(args.query_month), header=True)
	records = records.join(devices, on=['imei'], how='inner').cache()

	devices = records.repartition(1000, ['imei']).groupBy(['imei', 'data_date']).agg(\
		F.count(F.lit(1)).alias('installed_app_count'))
	devices = devices.groupBy(['imei']).agg(F.mean('installed_app_count').alias('daily_installed_app_count'))
	devices = devices.select(F.mean('daily_installed_app_count').alias('avg_daily_installed_app_count')).collect()
	result['avg_daily_installed_app_count'] = devices[0]['avg_daily_installed_app_count']

	apps = records.repartition(1000, ['app_package']).groupBy(['app_package', 'data_date']).agg(\
		F.count(F.lit(1)).alias('installed_device_count'))
	apps = apps.groupBy(['app_package']).agg(F.mean('installed_device_count').alias('daily_installed_device_count'))
	apps.repartition(100).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installed/app_rank/{0}'.format(args.query_month), header=True)
	apps = apps.select(F.mean('daily_installed_device_count').alias('avg_daily_installed_device_count')).collect()
	result['avg_daily_installed_device_count'] = apps[0]['avg_daily_installed_device_count']

	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installed/{0}'.format(args.query_month), header=True)