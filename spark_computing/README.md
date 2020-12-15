## Spark Computing
### xgboost4j on Spark (Python wrapper)
* Please use Spark 3.0 (or Scala 2.12.x) for better compatibility and make sure sparkxgb.zip and related official xgboost4j jars prepared
#### Quick start  
`cd xgboost/`
`/opt/spark-3.0.1-bin-hadoop-2.6/bin/spark-submit --py-files sparkxgb.zip --jars xgboost4j_2.12-1.2.1.jar,xgboost4j-spark_2.12-1.2.1.jar xgb_spark_test.py --is_saving`

### Community detection and matrix factorization
