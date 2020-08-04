# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from datetime import datetime, timedelta
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import argparse

def retrieveAllRecords(spark, fr, to, os):
	sql = """
		select
			msg_id
		from
			edw.msg_log
		where
			data_date between '{0}' and '{1}'
			and from_unixtime(itime, 'yyyyMMdd') between '{0}' and '{1}'
			and platform = '{2}'
	""".format(fr, to, os)
	print(sql)
	records = spark.sql(sql)
	return records

def retrieveRecRecords(spark, fr, to, os):
	sql = """
		select
			msg_id
		from
			edw.msg_receive_log
		where
			data_date between '{0}' and '{1}'
			and from_unixtime(itime, 'yyyyMMdd') between '{0}' and '{1}'
			and platform = '{2}'
	""".format(fr, to, os)
	print(sql)
	records = spark.sql(sql)
	return records

def retrieveClkRecords(spark, fr, to, os):
	sql = """
		select
			msg_id
		from
			edw.msg_click_log
		where
			data_date between '{0}' and '{1}'
			and from_unixtime(itime, 'yyyyMMdd') between '{0}' and '{1}'
			and platform = '{2}'
	""".format(fr, to, os)
	print(sql)
	records = spark.sql(sql)
	return records

def retrieveTargets(spark, fr, to):
	sql = """
		select
			msg_id,
			uid
		from
			edw.msg_target_log 
		where
			data_date between '{0}' and '{1}'
	""".format(fr, to)
	print(sql)
	targets = spark.sql(sql)
	return targets

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

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('./config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Calculate_MSG_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str)
	parser.add_argument('--to', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	args = parser.parse_args()

	print('====> Start calculation')
	all_records = retrieveAllRecords(spark, args.fr, args.to, args.os)
	rec_records = retrieveRecRecords(spark, args.fr, args.to, args.os)
	clk_records = retrieveClkRecords(spark, args.fr, args.to, args.os)
	clk_records = clk_records.withColumn('msg_type', F.lit(1))
	rec_records = rec_records.join(clk_records, on=['msg_id'], how='left_outer')
	rec_records = rec_records.withColumn('msg_type', F.when(rec_records.msg_type.isNull(), 0).otherwise(rec_records.msg_type))
	all_records = all_records.join(rec_records, on=['msg_id'], how='left_outer')
	all_records = all_records.withColumn('msg_type', F.when(rec_records.msg_type.isNull(), 2).otherwise(rec_records.msg_type))
	targets = retrieveTargets(spark, args.fr, args.to)
	all_records = all_records.join(targets, on=['msg_id'], how='inner')
	uids = retrieveUidInfo(spark, args.fr, args.to, args.os)
	all_records = all_records.join(uids, on=['uid'], how='left_outer')
