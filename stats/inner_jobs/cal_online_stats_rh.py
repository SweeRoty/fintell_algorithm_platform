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

def retrieveOnlineRecords(spark, fr, to, os):
	sql = """
		select
			md5(cast(uid as string)) uid,
			stat_uid_ol_cnt online_count_per_day
		from
			ronghui_mart.rh_base_user_02_uid
		where
			data_date between '{0}' and '{1}'
			--and from_unixtime(itime, 'yyyyMMdd') between '{0}' and '{1}'
			and platform = '{2}'
	""".format(fr, to, os)
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
			.appName('RLab_Stats_Report___Cal_Online_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	args = parser.parse_args()
	fr = args.query_month+'01'
	to = args.query_month+str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])

	print('====> Start calculation')
	result = {}

	uids = retrieveUidInfo(spark, to, args.os)
	if args.os == 'i':
		uid_idfa = retrieveUidIDFA(spark, to)
		apps = retrieveAppInfo(spark)
		uid_idfa = uid_idfa.join(apps, on=['app_key'], how='inner').drop('app_key').select(['uid', 'app_package', 'device_id'])
		uids = uids.union(uids)

	records = retrieveOnlineRecords(spark, fr, to, args.os)
	records = records.join(uids, on=['uid'], how='inner').cache()

	devices = records.repartition(1000, ['device_id']).groupBy(['device_id']).agg(\
		F.sum('online_count_per_day').alias('device_online_times'), \
		F.approx_count_distinct('app_package', rsd=0.05).alias('device_online_app_count'))
	devices = devices.select(\
		F.count(F.lit(1)).alias('online_device_count'), \
		F.mean('device_online_times').alias('avg_online_times_per_device'), \
		F.mean('device_online_app_count').alias('avg_online_app_per_device')).collect()
	result['online_device_count'] = devices[0]['online_device_count']
	result['avg_online_times_per_device'] = devices[0]['avg_online_times_per_device']
	result['avg_online_app_per_device'] = devices[0]['avg_online_app_per_device']

	apps = records.repartition(1000, ['app_package']).groupBy(['app_package']).agg(\
		F.sum('online_count_per_day').alias('app_online_times'), \
		F.approx_count_distinct('device_id', rsd=0.05).alias('app_online_device_count'))
	apps = apps.select(\
		F.count(F.lit(1)).alias('online_app_count'), \
		F.mean('app_online_times').alias('avg_online_times_per_app'), \
		F.mean('app_online_device_count').alias('avg_online_device_per_app')).collect()
	result['online_app_count'] = apps[0]['online_app_count']
	result['avg_online_times_per_app'] = apps[0]['avg_online_times_per_app']
	result['avg_online_device_per_app'] = apps[0]['avg_online_device_per_app']

	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('hgy/rlab_stats_report/online/{0}/{1}'.format(args.os, args.query_month), header=True)