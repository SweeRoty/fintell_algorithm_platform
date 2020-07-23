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
			and imei is not null
			and imei != ''
			and package is not null
			and package != ''
	""".format(fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

def loadSampledDevices(spark, query_month):
	sql = """
		select
			imei
		from
			ronghui.hgy_01
		where
			data_date = '{0}'
	""".format(query_month)
	print(sql)
	devices = spark.sql(sql)
	return devices

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
			.appName('RLab_Stats_Report___Cal_Installed_Stats_for_Device') \
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
	'''
	records = records.withColumn('sampling_index', F.col('imei')%25)
	records = records.sampleBy('sampling_index', fractions={1:1}, seed=1003).drop('sampling_index')
	devices = spark.read.csv('/user/ronghui_safe/hgy/rlab_stats_report/sampled_devices/{0}'.format(args.query_month), header=True)
	'''
	devices = loadSampledDevices(spark, args.query_month)
	records = records.join(devices, on=['imei'], how='inner')

	devices = records.repartition(750, ['imei']).rdd.map(lambda row: ('{0}_{1}'.format(row['imei'], row['data_date']), 1)).reduceByKey(lambda x, y: x+y)
	devices = devices.map(lambda t: (t[0].split('_')[0], (t[1], 1))).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda t: t[0]*1.0/t[1])
	device_stats = devices.map(lambda t: (t[1], 1)).reduce(lambda a, b: (a[0]+b[0], a[1]+b[1]))
	result['avg_daily_installed_app_count'] = device_stats[0]*1.0/device_stats[1]

	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installed/{0}'.format(args.query_month), header=True)