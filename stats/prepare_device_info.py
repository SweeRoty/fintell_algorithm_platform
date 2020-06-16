# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import argparse

def retrieveActiveDevices(spark, query_date):
	sql = """
		select
			imei
		from
			ronghui_mart.rh_online_imei_3m_android
		where
			data_date = '{0}'
	""".format(query_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def retrieveProperties(spark, query_date):
	sql = """
		select
			imei,
			model,
			brand,
			price
		from
			ronghui_mart.rt_device_prop
		where
			data_date = '{0}'
			and platform = 'Android'
	""".format(query_date)
	print(sql)
	props = spark.sql(sql)
	return props

def retrieveISPs(spark, query_date):
	sql = """
		select
			imei,
			isp
		from
			ronghui_mart.rt_device_isp
		where
			data_date = '{0}'
	""".format(query_date)
	print(sql)
	isps = spark.sql(sql)
	return isps

def retrieveVersions(spark, query_date):
	sql = """
		select
			imei,
			sys_ver
		from
			ronghui.register_user_log
		where
			data_date <= '{0}'
			and platform = 'a'
	""".format(query_date)
	print(sql)
	vers = spark.sql(sql)
	return vers

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('./config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Prepare_Device_Info') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str)
	args = parser.parse_args()
	last_day = str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])
	query_date = args.query_month+last_day

	print('====> Start calculation')
	active_devices = retrieveActiveDevices(spark, query_date)
	props = retrieveProperties(spark, query_date)
	active_devices = active_devices.join(props, on=['imei'], how='left_outer')
	isps = retrieveISPs(spark, query_date)
	active_devices = active_devices.join(isps, on=['imei'], how='left_outer')
	vers = retrieveVersions(spark, query_date)
	vers = vers.withColumn('android_version', F.regexp_extract('sys_ver', '^(\d+)\.', 1)).drop('sys_ver')
	active_devices = active_devices.join(vers, on=['imei'], how='left_outer')
	active_devices.repartition(100).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/device/{0}'.format(args.query_month), header=True)
