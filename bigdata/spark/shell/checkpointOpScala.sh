spark-submit \
--class com.coding.bigdata.spark.CheckpointOpScala \
--master yarn \
--deploy-mode cluster \
--executor-memory 1G \
--num-executors 1 \
~/bigdata/spark/lib/spark-learning-1.0-SNAPSHOT-jar-with-dependencies.jar
