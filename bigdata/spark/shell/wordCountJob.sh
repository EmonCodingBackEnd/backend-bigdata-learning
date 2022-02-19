spark-submit \
--class com.coding.bigdata.spark.WordCountScala \
--master yarn \
--deploy-mode client \
--executor-memory 1G \
--num-executors 1 \
~/bigdata/spark/lib/spark-learning-1.0-SNAPSHOT-jar-with-dependencies.jar \
hdfs://emon:8020/custom/data/spark/hello.txt
