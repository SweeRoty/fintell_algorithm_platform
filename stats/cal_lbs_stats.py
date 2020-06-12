# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from datetime import datetime, timedelta
from itertools import product
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import argparse

def retrieveUidInfo(spark, to):
	sql = """
		select
			distinct uid,
			package_name app_package,
			imei
		from
			edw.user_register_log
		where
			data_date <= '{0}'
	""".format(to)
	print(sql)
	uids = spark.sql(sql)
	return uids

def retrieveLBSRecords(spark, fr, to):
	sql = """
		select
			uid,
			platform os,
			coordinate_source type
		from
			edw.active_user_log
		where
			data_date between '{0}' and '{1}'
			and itime between unix_timestamp('{0}', 'YYYYmmdd') and unix_timestamp('{1}', 'YYYYmmdd')
	""".format(fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('./config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Prepare_LBS_Data') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_date', type=str)
	parser.add_argument('--forward', type=int)
	args = parser.parse_args()
	to = datetime.strptime(args.query_date, '%Y%m%d').date()+timedelta(days=args.forward)
	to = to.strftime('%Y%m%d')

	print('====> Start calculation')
	types = ['gps', 'wifi', 'cell', 'ip']
	result = {}
	uids = retrieveUidInfo(spark, to).repartition(5000, ['uid'])
	records = retrieveLBSRecords(spark, args.query_date, to).repartition(5000, ['uid'])
	records = records.join(uids, on=['uid'], how='left_outer').where(records.os == 'a').drop('os').cache() # only android devices
	result['lbs_point_count'] = records.count()
	result['lbs_gps_point_count'] = records.where(records.type == 'gps').count()
	result['lbs_wifi_point_count'] = records.where(records.type == 'wifi').count()
	result['lbs_cell_point_count'] = records.where(records.type == 'cell').count()
	result['lbs_ip_point_count'] = records.where(records.type == 'ip').count()

	general_stats = records.groupBy(['type']).agg(F.countDistinct('imei').alias('lbs_device_count'), \
		F.countDistinct('app_package').alias('lbs_app_count')).cache()
	for t in types:
		row = general_stats.where(general_stats.type == t).collect()[0]
		result['lbs_{0}_device_count'.format(t)] = row['lbs_device_count']
		result['lbs_{0}_app_count'.format(t)] = row['lbs_app_count']
	general_stats.unpersist()

	devices = records.groupBy(['imei', 'type']).agg(F.count(F.lit(1)).alias('device_lbs_times'), \
		F.countDistinct('app_package').alias('device_lbs_app_count'))
	devices_stats = devices.groupBy(['type']).agg(F.mean('device_lbs_times').alias('avg_lbs_times_pd'), \
		F.mean('device_lbs_app_count').alias('avg_lbs_app_count_pd')).cache()
	for t in types:
		row = devices_stats.where(devices_stats.type == t).collect()[0]
		result['avg_lbs_times_pd_{0}'.format(t)] = row['avg_lbs_times_pd']
		result['avg_lbs_app_count_pd_{0}'.format(t)] = row['avg_lbs_app_count_pd']
	devices.unpersist()

	apps = records.groupBy(['app_package', 'type']).agg(F.count(F.lit(1)).alias('app_lbs_times'), \
		F.countDistinct('imei').alias('app_lbs_device_count'))
	apps_stats = apps.groupBy(['type']).agg(F.mean('app_lbs_times').alias('avg_lbs_times_pa'), \
		F.mean('app_lbs_device_count').alias('avg_lbs_device_count_pa')).cache()
	for t in types:
		row = apps_stats.where(apps_stats.type == t).collect()[0]
		result['avg_lbs_times_pa_{0}'.format(t)] = row['avg_lbs_times_pa']
		result['avg_lbs_device_count_pa_{0}'.format(t)] = row['avg_lbs_device_count_pa']
	apps_stats.unpersist()
	
	result = spark.createDataFrame([result])
	result = result.withColumn('data_date', F.lit(args.query_date))
	result.repartition(1).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/lbs/{0}'.format(args.query_date), header=True)