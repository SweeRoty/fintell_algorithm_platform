# -*- coding: utf-8 -*-

from __future__ import division
from ConfigParser import RawConfigParser
from pyspark.ml.feature import Bucketizer
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import argparse

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Cal_Device_Info') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	
	print('====> Start calculation')
	bucketizer = Bucketizer(splits=[0, 500, 1000, 1500, 2000, 2500, 3000, 4000, 5000, float('Inf')], \
		inputCol='price', outputCol='price_bin')
	for month in ['202001', '202002', '202003', '202004', '202005']:
		active_devices = spark.read.csv('hgy/rlab_stats_report/device_info/{0}'.format(month), header=True).cache()
		android_10_count = active_devices.select(F.col('android_version').cast('int').alias('android_version')).where(\
			F.col('android_version') == 10).count()
		print('Android 10 count is: {0}'.format(android_10_count))
		print('Android 10 ratio is: {0}'.format(round(android_10_count/active_devices.count(), 10)))
		isp_stats = active_devices.where((active_devices.isp.contains('移动')) | (active_devices.isp.contains('联通')) | (active_devices.isp.contains('电信')))
		isp_stats = isp_stats.groupBy(['isp']).agg(F.count(F.lit(1)).alias('isp_count')).collect()
		print(isp_stats)
		active_devices = active_devices.where(active_devices.price.isNotNull()).select(F.col('price').cast('int').alias('price'))
		active_devices = bucketizer.setHandleInvalid('keep').transform(active_devices)
		price_stats = active_devices.groupBy(['price_bin']).agg(F.count(F.lit(1)).alias('price_bin_count')).collect()
		print(price_stats)