## Spark Computing
### Section 1: xgboost4j on Spark (Python wrapper)
#### Note
* Please use Spark 3.0 (or Scala 2.12.x) for better compatibility and make sure sparkxgb.zip and related official xgboost4j jars prepared
#### Quick start  
1. `cd xgboost/`\
2. `/opt/spark-3.0.1-bin-hadoop-2.6/bin/spark-submit --py-files sparkxgb.zip --jars xgboost4j_2.12-1.2.1.jar,xgboost4j-spark_2.12-1.2.1.jar xgb_spark_test.py --is_saving`

### Section2 : community detection and matrix factorization
