spark-submit \
--class com.coding.bigdata.spark.TopNScala \
--packages com.alibaba:fastjson:1.2.79 \
--master yarn \
--deploy-mode client \
--executor-memory 1G \
--num-executors 1 \
~/bigdata/spark/lib/spark-learning-1.0-SNAPSHOT-jar-with-dependencies.jar
