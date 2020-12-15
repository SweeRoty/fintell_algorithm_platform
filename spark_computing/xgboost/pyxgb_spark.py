# -*- coding: utf-8 -*-

import argparse
import configparser

#from graphframes.examples import Graphs

from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType
from sparkxgb import XGBoostClassifier

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = configparser.ConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Computing___XGB') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--label_col', type=str, default='Pseudo_tag')
	parser.add_argument('--cv', dest='tuning_mode', action='store_true', default=False)
	parser.add_argument('--cv_splits', type=int, default=5)
	parser.add_argument('--tv_ratio', type=float, default=0.7)
	parser.add_argument('--is_saving', action='store_true', default=False)
	args = parser.parse_args()

	print('====> Start computation')
	"""
	g = Graphs(spark).friends()
	g.vertices.show()
	"""

	train_data = spark.read.csv('/user/ronghui_safe/hgy/spark/train_data.csv', header=True)
	label = args.label_col
	n_pos = train_data.where(F.col(label) == 1).count()
	n_neg = train_data.where(F.col(label) == 0).count()
	print('----> The number of pos samples is {:d}, the number of neg samples is {:d} and their ratio is {:.4f}'.format(n_pos, n_neg, n_neg/n_pos))

	features = [col for col in train_data.columns if col != label]
	for feature in features:
		train_data = train_data.withColumn(feature, F.col(feature).cast(FloatType()))
	train_data = train_data.withColumn(label, F.col(label).cast(IntegerType()))

	vectorAssembler = VectorAssembler().setInputCols(features).setOutputCol('features_vec')
	params = {
				'objective': 'binary:logistic',
				'eta': 0.1,
				'numRound': 200,
				#'maxDepth': 5,
				'maxLeaves': 127,
				#'minChildWeight': 30.0,
				'subsample': 0.7,
				'colsampleBytree':0.7,
				'gamma': 0.0,
				#'missing': 0.0,
				'treeMethod': 'hist',
				'growPolicy': 'depthwise',
				'scalePosWeight': n_neg/n_pos,
				'nthread': 1,
				'numWorkers': 10,
				#'numEarlyStoppingRounds': 10,
				'maximizeEvaluationMetrics':True
	}
	xgboost = XGBoostClassifier(**params).setLabelCol(label).setFeaturesCol('features_vec')
	pipeline = Pipeline().setStages([vectorAssembler, xgboost])

	evaluator = BinaryClassificationEvaluator(labelCol=label, rawPredictionCol='probability')
	paramGrid = ParamGridBuilder() \
				.addGrid(xgboost.maxDepth, [3, 4]) \
				.addGrid(xgboost.minChildWeight, [1e1, 1e2, 1e3]) \
				.build()

	model = None
	if args.tuning_mode:
		tvs = TrainValidationSplit() \
				.setEstimator(pipeline) \
				.setEstimatorParamMaps(paramGrid) \
				.setEvaluator(evaluator) \
				.setTrainRatio(args.tv_ratio)
		model = tvs.fit(train_data)
		print('----> The validation metrics is {}'.format(model.validationMetrics))
	else:
		cv = CrossValidator() \
				.setEstimator(pipeline) \
				.setEstimatorParamMaps(paramGrid) \
				.setEvaluator(evaluator) \
				.setNumFolds(args.cv_splits)
		model = cv.fit(train_data)
		print('----> The average validation metrics is {}'.format(model.avgMetrics))

	print(evaluator.evaluate(model.transform(train_data)))

	if args.is_saving:
		model.write().save('/user/ronghui_safe/hgy/spark/xgb_test.model')