# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from datetime import datetime, timedelta
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

def retrieveOnlineRecords(spark, fr, to, os):
	sql = """
		select
			uid
		from
			edw.online_user_log
		where
			data_date between '{0}' and '{1}'
			and from_unixtime(itime, 'yyyyMMdd') between '{0}' and {1}'
			and platform = '{2}'
	""".format(fr, to, os)
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
			.appName('RLab_Stats_Report___Cal_Online_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str)
	parser.add_argument('--to', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	args = parser.parse_args()
	assert args.fr <= args.to

	print('====> Start calculation')
	result = {}
	uids = retrieveUidInfo(spark, args.to, args.os)
	records = retrieveOnlineRecords(spark, args.fr, args.to, args.os)
	records = records.join(uids, on=['uid'], how='left_outer').cache()

	devices = records.repartition(5000, ['device_id']).groupBy(['device_id']).agg(\
		F.count(F.lit(1)).alias('device_online_times'), \
		F.approx_count_distinct('app_package', rsd=0.05).alias('device_online_app_count'))
	devices = devices.select(\
		F.count(F.lit(1)).alias('online_device_count'), \
		F.mean('device_online_times').alias('avg_online_times_per_device'), \
		F.mean('device_online_app_count').alias('avg_online_app_per_device')).collect()
	result['online_device_count'] = devices[0]['online_device_count']
	result['avg_online_times_per_device'] = devices[0]['avg_online_times_per_device']
	result['avg_online_app_per_device'] = devices[0]['avg_online_app_per_device']

	apps = records.repartition(5000, ['app_package']).groupBy(['app_package']).agg(\
		F.count(F.lit(1)).alias('app_online_times'), \
		F.approx_count_distinct('device_id', rsd=0.05).alias('app_online_device_count'))
	apps = apps.select(\
		F.count(F.lit(1)).alias('online_app_count'), \
		F.mean('app_online_times').alias('avg_online_times_per_app'), \
		F.mean('app_online_device_count').alias('avg_online_device_per_app')).collect()
	result['online_app_count'] = apps[0]['online_app_count']
	result['avg_online_times_per_app'] = apps[0]['avg_online_times_per_app']
	result['avg_online_device_per_app'] = apps[0]['avg_online_device_per_app']

	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/online/{0}'.format(args.fr), header=True)