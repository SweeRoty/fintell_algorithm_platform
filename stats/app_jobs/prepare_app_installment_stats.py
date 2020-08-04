# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def loadSampledDevices(spark, data_date):
	sql = """
		select
			imei
		from
			ronghui.hgy_01
		where
			data_date = '{0}'
	""".format(data_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def retrieveRawRecords(spark, fr, to):
	sql = """
		select
			distinct md5(cast(imei as string)) imei,
			package app_package,
			status
		from
			edw.app_list_install_uninstall_fact
		where
			data_date between '{0}' and '{1}'
			and status != 1
			and from_unixtime(last_report_time, 'yyyyMMdd') = data_date
			and imei is not null
			and imei != ''
			and package is not null
			and package != ''
	""".format(fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

def transform_to_row(t):
	app_package, status = t[0].split('sweeroty')
	return Row(app_package=app_package, status=int(status), count=int(t[1]))

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('./config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Prepare_APP_Installment_Stats') \
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
	records = retrieveRawRecords(spark, fr, to)
	devices = loadSampledDevices(spark, args.query_month)
	records = records.join(devices, on=['imei'], how='inner')
	apps = records.drop('imei').repartition(1000, 'app_package').rdd.map(lambda row: ('{0}sweeroty{1}'.format(row['app_package'].encode('utf-8'), row['status']), 1)).reduceByKey(lambda x, y: x+y).map(transform_to_row).toDF()
	apps.repartition(10).write.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installment/app/{0}'.format(args.query_month), header=True)