# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from datetime import datetime, timedelta
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

def retrieveOnlineRecords(spark, fr, to):
	sql = """
		select
			platform os,
			uid
		from
			edw.online_user_log
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
			.appName('RLab_Stats_Report___Prepare_Online_Data') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_date', type=str)
	parser.add_argument('--forward', type=int)
	args = parser.parse_args()
	to = datetime.strptime(args.query_date, '%Y%m%d').date()+timedelta(days=args.forward)
	to = to.strftime('%Y%m%d')

	print('====> Start calculation')
	result = {}
	uids = retrieveUidInfo(spark, to).repartition(5000, ['uid'])
	records = retrieveOnlineRecords(spark, args.query_date, to).repartition(5000, ['uid'])
	records = records.join(uids, on=['uid'], how='left_outer').cache()

	devices = records.groupBy(['imei', 'os']).agg(F.count(F.lit(1)).alias('device_online_times'), \
		F.countDistinct('app_package').alias('device_online_app_count')).cache()
	devices_stats = devices.where(devices.os == 'a').select(F.count(F.lit(1)).alias('android_online_device_count'), \
		F.mean('device_online_times').alias('android_avg_online_times_pd'), \
		F.mean('device_online_app_count').alias('android_avg_online_app_pd')).collect()
	result['android_online_device_count'] = devices_stats[0]['android_online_device_count']
	result['android_avg_online_times_pd'] = devices_stats[0]['android_avg_online_times_pd']
	result['android_avg_online_app_pd'] = devices_stats[0]['android_avg_online_app_pd']
	'''
	devices_stats = devices.where(devices.os == 'i').select(F.count(F.lit(1)).alias('ios_online_device_count'), \
		F.mean('device_online_times').alias('ios_avg_online_times_pd'), \
		F.mean('device_online_app_count').alias('ios_avg_online_app_pd')).collect()
	result['ios_online_device_count'] = devices_stats[0]['ios_online_device_count']
	result['ios_avg_online_times_pd'] = devices_stats[0]['ios_avg_online_times_pd']
	result['ios_avg_online_app_pd'] = devices_stats[0]['ios_avg_online_app_pd']
	'''
	devices.unpersist()

	apps = records.groupBy(['app_package', 'os']).agg(F.count(F.lit(1)).alias('app_online_times'), \
		F.countDistinct('imei').alias('app_online_device_count')).cache()
	apps_stats = apps.where(apps.os == 'a').select(F.count(F.lit(1)).alias('android_online_app_count'), \
		F.mean('app_online_times').alias('android_avg_online_times_pa'), \
		F.mean('app_online_device_count').alias('android_avg_online_device_pa')).collect()
	result['android_online_app_count'] = apps_stats[0]['android_online_app_count']
	result['android_avg_online_times_pa'] = apps_stats[0]['android_avg_online_times_pa']
	result['android_avg_online_device_pa'] = apps_stats[0]['android_avg_online_device_pa']
	'''
	apps_stats = apps.where(apps.os == 'i').select(F.count(F.lit(1)).alias('ios_online_app_count'), \
		F.mean('app_online_times').alias('ios_avg_online_times_pa'), \
		F.mean('app_online_device_count').alias('ios_avg_online_device_pa')).collect()
	result['ios_online_app_count'] = apps_stats[0]['ios_online_app_count']
	result['ios_avg_online_times_pa'] = apps_stats[0]['ios_avg_online_times_pa']
	result['ios_avg_online_device_pa'] = apps_stats[0]['ios_avg_online_device_pa']
	'''
	apps.unpersist()

	result = spark.createDataFrame([result])
	result = result.withColumn('data_date', F.lit(args.query_date))
	result.repartition(1).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/online/{0}'.format(args.query_date), header=True)