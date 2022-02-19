spark-submit \
--class com.coding.bigdata.spark.MoreParallelismScala \
--master yarn \
--deploy-mode client \
--executor-memory 1G \
--num-executors 3 \
--executor-cores 2 \
--conf "spark.default.parallelism=10" \
~/bigdata/spark/lib/spark-learning-1.0-SNAPSHOT-jar-with-dependencies.jar
