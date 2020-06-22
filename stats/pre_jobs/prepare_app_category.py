# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import argparse

def retrieveAppCategory(spark):
	sql = """
		select
			package app_package
		from
			ronghui_mart.app_info
		where
			(class_one is null or class_one in ('N', 'NULL', ''))
			and (class_two is null or class_two in ('N', 'NULL', ''))
			and (class_three is null or class_three in ('N', 'NULL', ''))
	"""
	apps = spark.sql(sql)
	return apps

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Prepare_App_Category_Data') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')

	print('====> Start calculation')
	apps = retrieveAppCategory(spark)
	apps = apps.withColumn('unclassified', F.lit(1))
	apps.repartition(1).write.csv('./hgy/rlab_stats_report/unclassified_app', header=True)