# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from datetime import datetime, timedelta
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import argparse

def retrieveRawRecords(spark, fr, to):
	sql = """
		select
			imei,
			package app_package,
			name app_name,
			status
		from
			edw.app_list_install_uninstall_fact
		where
			data_date between '{0}' and '{1}'
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
			.appName('RLab_Stats_Report___Prepare_Installment_Data') \
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
	records = retrieveRawRecords(spark, args.query_date, to).repartition(5000).cache()
	result['newly_install_count'] = records.where(records.status == 2).count()
	result['uninstall_count'] = records.where(records.status == 0).count()

	devices = records.groupBy(['imei', 'status']).agg(F.count(F.lit(1)).alias('device_install_times'), \
													F.countDistinct('app_package').alias('device_install_app_count')).cache()
	result['newly_installed_device_count'] = devices.where(devices.status == 2).count()	
	devices_stats = devices.where(devices.status == 2).select(F.mean('device_install_times').alias('avg_newly_install_times_pd'), \
															F.mean('device_install_app_count').alias('avg_newly_installed_app_pd')).collect()
	result['avg_newly_install_times_pd'] = devices_stats[0]['avg_newly_install_times_pd']
	result['avg_newly_installed_app_pd'] = devices_stats[0]['avg_newly_installed_app_pd']
	result['uninstalled_device_count'] = devices.where(devices.status == 0).count()
	devices_stats = devices.where(devices.status == 0).select(F.mean('device_install_times').alias('avg_uninstall_times_pd'), \
															F.mean('device_install_app_count').alias('avg_uninstalled_app_pd')).collect()
	result['avg_uninstall_times_pd'] = devices_stats[0]['avg_uninstall_times_pd']
	result['avg_uninstalled_app_pd'] = devices_stats[0]['avg_uninstalled_app_pd']
	devices.unpersist()

	apps = records.groupBy(['app_package', 'status']).agg(F.count(F.lit(1)).alias('app_install_times'), \
														F.countDistinct('imei').alias('app_install_device_count')).cache()
	result['newly_installed_app_count'] = apps.where(apps.status == 2).count()	
	apps_stats = apps.where(apps.status == 2).select(F.mean('app_install_times').alias('avg_newly_install_times_pa'), \
													F.mean('app_install_device_count').alias('avg_newly_installed_device_pa')).collect()
	result['avg_newly_install_times_pa'] = apps_stats[0]['avg_newly_install_times_pa']
	result['avg_newly_installed_device_pa'] = apps_stats[0]['avg_newly_installed_device_pa']
	result['uninstalled_app_count'] = apps.where(apps.status == 0).count()
	apps_stats = apps.where(apps.status == 0).select(F.mean('app_install_times').alias('avg_uninstall_times_pa'), \
													F.mean('app_install_device_count').alias('avg_uninstalled_device_pd')).collect()
	result['avg_uninstall_times_pa'] = apps_stats[0]['avg_uninstall_times_pa']
	result['avg_uninstalled_device_pd'] = apps_stats[0]['avg_uninstalled_device_pd']
	apps.unpersist()

	result = spark.createDataFrame([result])
	result = result.withColumn('data_date', F.lit(args.query_date))
	result.repartition(1).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installment/{0}'.format(args.query_date), header=True)