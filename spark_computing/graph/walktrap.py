# -*- coding:utf-8 -*-

from __future__ import division

from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.linalg import Vectors as V
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry
from pyspark.sql import Row, SparkSession
import pyspark.sql.functions as F

import argparse

def inflate(row):
	results = [(row['fr']-1, (row['to']-1, row['weight'])), (row['to']-1, (row['fr']-1, row['weight']))]
	return results

def normalize(t):
	new_entries = []
	total_weight = sum([weight for _, weight in t[1]])
	for index, weight in t[1]:
		new_entries.append(MatrixEntry(t[0], index, round(weight/total_weight, 6)))
	return new_entries

def transition(t):
	n = t[1].size
	arr = [0]*n
	for indexedRow in adj.value:
		index = indexedRow.index
		vector = indexedRow.vector
		arr[index] = t[1].dot(vector)
	return (t[0], Vectors.dense(arr))

def transform2row(t):
	arr = t[1][0].toArray()
	arr = arr/t[1][1]
	return Row(node=t[0], features=V.dense(arr))

def inflate_row(row):
	results = [Row(node=row['fr']-1, weight=row['weight']), Row(node=row['to']-1, weight=row['weight'])]
	return results

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Small_WalkTrap') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--t', type=int)
	parser.add_argument('--k', type=int)
	parser.add_argument('--filename', type=str)
	args = parser.parse_args()

	edges = spark.read.csv('/user/ronghui_safe/hgy/app/community_detection/{0}'.format(args.filename), header=True, inferSchema=True)
	P = CoordinateMatrix(edges.rdd.flatMap(inflate).groupByKey(numPartitions=200).flatMap(normalize))
	adj = P.transpose().toIndexedRowMatrix()
	adj = sc.broadcast(adj.rows.collect())
	P = P.toIndexedRowMatrix().rows.map(lambda row: (row.index, row.vector))

	assert args.t >= 1
	for i in range(args.t-1):
		P = P.map(transition)
	#adj.destroy()

	#P = P.map(transform2row).toDF()
	weights = edges.rdd.flatMap(inflate_row).toDF().repartition(200).groupBy('node').agg(F.sqrt(F.sum(F.col('weight'))).alias('total_weight')).rdd.map(lambda row: (row['node'], row['total_weight']))
	training_df = P.join(weights).map(transform2row).toDF()

	bkm = BisectingKMeans(maxIter=100, seed=1989, k=args.k, minDivisibleClusterSize=1) #weightCol='total_weight'
	mdl = bkm.fit(training_df)
	clusters = mdl.transform(training_df)
	clusters.repartition(1).write.csv('/user/ronghui_safe/hgy/app/community_detection/{0}_with_clusters'.format(args.filename), header=True)