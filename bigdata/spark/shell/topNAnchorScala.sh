spark-submit \
--class com.coding.bigdata.spark.sql.TopNAnchorScala \
--master yarn \
--deploy-mode cluster \
--executor-memory 1G \
--num-executors 3 \
--executor-cores 2 \
--conf "spark.default.parallelism=6" \
~/bigdata/spark/lib/spark-learning-1.0-SNAPSHOT-jar-with-dependencies.jar
