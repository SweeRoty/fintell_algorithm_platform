# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

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

def retrieveUidIDFA(spark, to):
	sql = """
		select
			distinct uid,
			idfa device_id,
			app_key
		from
			ronghui.uid2idfa_fact
		where
			data_date <= '{0}'
	""".format(to)
	print(sql)
	uids = spark.sql(sql)
	return uids

def retrieveAppInfo(spark):
	sql = """
		select
			package app_package,
			app_key
		from
			ronghui_mart.app_info
	"""
	print(sql)
	apps = spark.sql(sql)
	return apps

def retrieveLBSRecords(spark, fr, to, os):
	table = 'ronghui_mart.rh_lbs_wifi_daily' if os == 'a' else 'ronghui_mart.user_location_log_daily_ios'
	uid_col = 'uid' if os == 'a' else 'md5(cast(uid as string))'
	sql = """
		select
			{0} uid,
			coordinate_source type
		from
			{1}
		where
			data_date between '{2}' and '{3}'
			and from_unixtime(itime, 'yyyyMMdd') between '{2}' and '{3}'
	""".format(uid_col, table, fr, to)
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
	localConf.read('../config')
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
	parser.add_argument('--query_month', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	args = parser.parse_args()
	fr = args.query_month+'01'
	to = args.query_month+str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])

	print('====> Start calculation')
	types = ['GPS', 'WIFI', 'CELL', 'IP']
	result = {}

	uids = retrieveUidInfo(spark, to, args.os)
	if args.os == 'i':
		uid_idfa = retrieveUidIDFA(spark, to)
		apps = retrieveAppInfo(spark)
		uid_idfa = uid_idfa.join(apps, on=['app_key'], how='inner').drop('app_key').select(['uid', 'app_package', 'device_id'])
		uids = uids.union(uids)
	else:
		devices = spark.read.csv('hgy/rlab_stats_report/active_devices/{0}/sampled_imei_list'.format(args.query_month), header=True)\
					.select(F.col('imei').alias('device_id'))
		uids = uids.join(devices, on=['device_id'], how='inner')

	records = retrieveLBSRecords(spark, fr, to, args.os)
	records = records.join(uids, on=['uid'], how='inner').cache()
	result['lbs_point_count'] = records.count()
	result['lbs_gps_point_count'] = records.where(records.type == 'GPS').count()
	result['lbs_wifi_point_count'] = records.where(records.type == 'WIFI').count()
	result['lbs_cell_point_count'] = records.where(records.type == 'CELL').count()
	result['lbs_ip_point_count'] = records.where(records.type == 'IP').count()

	devices = records.repartition(1000, ['device_id']).groupBy(['device_id', 'type']).agg(\
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

	apps = records.repartition(1000, ['app_package']).groupBy(['app_package', 'type']).agg(\
		F.count(F.lit(1)).alias('app_lbs_times'), \
		F.approx_count_distinct('device_id', rsd=0.05).alias('app_lbs_device_count')).cache()
	apps.repartition(100).write.csv('hgy/rlab_stats_report/lbs/app_rank/{0}/{1}'.format(args.os, args.query_month), header=True)
	for t in types:
		apps_stats = apps.where(apps.type == t).agg(\
			F.count(F.lit(1)).alias('lbs_app_count_{0}'.format(t)), \
			F.mean('app_lbs_times').alias('avg_lbs_times_per_app_{0}'.format(t)), \
			F.mean('app_lbs_device_count').alias('avg_lbs_device_per_app_{0}'.format(t))).collect()
		print('=========>', t, apps_stats)
		result['lbs_app_count_{0}'.format(t)] = apps_stats[0]['lbs_app_count_{0}'.format(t)]
		result['avg_lbs_times_per_app_{0}'.format(t)] = apps_stats[0]['avg_lbs_times_per_app_{0}'.format(t)]
		result['avg_lbs_device_per_app_{0}'.format(t)] = apps_stats[0]['avg_lbs_device_per_app_{0}'.format(t)]
	apps.unpersist()
	
	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('hgy/rlab_stats_report/lbs/{0}/{1}'.format(args.os, args.query_month), header=True)