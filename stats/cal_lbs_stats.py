# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from datetime import datetime, timedelta
from itertools import product
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import argparse

def retrieveUidInfo(spark, to, os):
	device = 'imei' if os == 'a' else 'idfa'
	sql = """
		select
			distinct uid,
			package_name app_package,
			{0} device_id
		from
			ronghui.register_user_log
		where
			data_date <= '{1}'
			and platform = '{2}'
	""".format(device, to, os)
	print(sql)
	uids = spark.sql(sql)
	return uids

def retrieveLBSRecords(spark, fr, to):
	sql = """
		select
			uid,
			coordinate_source type
		from
			edw.user_location_log
		where
			data_date between '{0}' and '{1}'
			and from_unixtime(itime, 'yyyyMMdd') between '{0}' and '{1}'
	""".format(fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

def transform_to_row(row_dict):
	global args
	row_dict['data_date'] = args.fr
	return Row(**row_dict)

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('./config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Cal_LBS_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str)
	parser.add_argument('--to', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	args = parser.parse_args()
	assert args.fr <= args.to

	print('====> Start calculation')
	types = ['GPS', 'WIFI', 'CELL', 'IP']
	result = {}
	uids = retrieveUidInfo(spark, args.to, args.os)
	records = retrieveLBSRecords(spark, args.fr, args.to)
	records = records.join(uids, on=['uid'], how='inner').cache()
	result['lbs_point_count'] = records.count()
	result['lbs_gps_point_count'] = records.where(records.type == 'GPS').count()
	result['lbs_wifi_point_count'] = records.where(records.type == 'WIFI').count()
	result['lbs_cell_point_count'] = records.where(records.type == 'CELL').count()
	result['lbs_ip_point_count'] = records.where(records.type == 'IP').count()

	devices = records.repartition(5000, ['device_id']).groupBy(['device_id', 'type']).agg(\
		F.count(F.lit(1)).alias('device_lbs_times'), \
		F.approx_count_distinct('app_package', rsd=0.05).alias('device_lbs_app_count')).cache()
	for t in types:
		devices_stats = devices.where(devices.type == t).agg(\
			F.count(F.lit(1)).alias('lbs_device_count_{0}'.format(t)), \
			F.mean('device_lbs_times').alias('avg_lbs_times_per_device_{0}'.format(t)), \
			F.mean('device_lbs_app_count').alias('avg_lbs_app_per_device_{0}'.format(t))).collect()
		print('=========>', t, devices_stats)
		result['lbs_device_count_{0}'.format(t)] = devices_stats[0]['lbs_device_count_{0}'.format(t)]
		result['avg_lbs_times_per_device_{0}'.format(t)] = devices_stats[0]['avg_lbs_times_per_device_{0}'.format(t)]
		result['avg_lbs_app_per_device_{0}'.format(t)] = devices_stats[0]['avg_lbs_app_per_device_{0}'.format(t)]
	devices.unpersist()

	apps = records.repartition(5000, ['app_package']).groupBy(['app_package', 'type']).agg(\
		F.count(F.lit(1)).alias('app_lbs_times'), \
		F.approx_count_distinct('device_id', rsd=0.05).alias('app_lbs_device_count')).cache()
	apps.write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/lbs/app_rank/{0}'.format(args.fr), header=True)
	for t in types:
		apps_stats = apps.where(apps.type == t).agg(\
			F.count(F.lit(1)).alias('lbs_app_count_{0}'.format(t)), \
			F.mean('app_lbs_times').alias('avg_lbs_times_per_app_{0}'.format(t)), \
			F.mean('app_lbs_device_count').alias('avg_lbs_device_per_app_{0}'.format(t))).collect()
		print('=========>', t, apps_stats)
		result['lbs_app_count_{0}'.format(t)] = devices_stats[0]['lbs_app_count_{0}'.format(t)]
		result['avg_lbs_times_per_app_{0}'.format(t)] = devices_stats[0]['avg_lbs_times_per_app_{0}'.format(t)]
		result['avg_lbs_device_per_app_{0}'.format(t)] = devices_stats[0]['avg_lbs_device_per_app_{0}'.format(t)]
	apps.unpersist()
	
	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/lbs/{0}'.format(args.fr), header=True)