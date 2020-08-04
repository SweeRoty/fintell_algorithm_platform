# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def retrieveScannedInfo(spark, fr, to):
	sql = """
		select
			imei,
			data_date
		from
			ronghui_mart.rh_stat_app_install_all
		where
			data_date between '{0}' and '{1}'
	""".format(fr, to)
	print(sql)
	scanned_devices = spark.sql(sql)
	return scanned_devices

def retrieveActionCounts(spark, to):
	sql = """
		select
			imei,
			cnt_app_30d_new,
			cnt_app_30d_delete
		from
			ronghui_mart.rh_stat_app_install_30d_all
		where
			data_date = '{0}'
	""".format(to)
	print(sql)
	action_counts = spark.sql(sql)
	return action_counts

def transform_to_row(row_dict):
	global args
	row_dict['data_date'] = args.query_month
	return Row(**row_dict)

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Cal_Installment_Stats_of_Devices') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str)
	args = parser.parse_args()
	fr = args.query_month+'01'
	to = args.query_month+str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])

	print('====> Start calculation')
	result = {}
	
	scanned_devices = retrieveScannedInfo(spark, fr, to)
	scanned_devices_stats = scanned_devices.groupBy(['data_date']).agg(F.count(F.lit(1)).alias('daily_device_count'))
	scanned_devices_stats = scanned_devices_stats.select(F.mean('daily_device_count').alias('avg_daily_device_count')).collect()
	result['avg_daily_device_count'] = scanned_devices_stats[0]['avg_daily_device_count']

	action_counts = retrieveActionCounts(spark, to)
	action_count_stats = action_counts.select(\
		F.mean('cnt_app_30d_new').alias('avg_installed_app_per_device_30'),\
		F.mean('cnt_app_30d_new').alias('avg_uninstalled_app_per_device_30')).collect()
	result['avg_installed_app_per_device_30'] = action_count_stats[0]['avg_installed_app_per_device_30']
	result['avg_uninstalled_app_per_device_30'] = action_count_stats[0]['avg_uninstalled_app_per_device_30']

	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('hgy/rlab_stats_report/installment/device/{0}'.format(args.query_month), header=True)