sqoop export \
--connect jdbc:mysql://emon:3306/sqoopdb?serverTimezone=UTC\&useSSL=false \
--username flyin \
--password Flyin@123 \
--table user2 \
--export-dir /sqoop/out2 \
--input-fields-terminated-by '\t' \
--update-key id \
--update-mode allowinsert
