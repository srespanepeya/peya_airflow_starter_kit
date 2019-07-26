#!/bin/bash
if [ -z "$1" ]
then
        echo "--->Begin ETL Load Coupons into S3"
        #PYSPARK_PYTHON=/usr/bin/python3 /home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode-ti:7077 --driver-memory 6G --driver-cores 4 --executor-memory 6G --conf spark.cores.max=4 /home/hduser/spark/apps/mkt_process_coupons_to_s3.py
        /home/hduser/spark/bin/spark-submit --master local[4] --driver-memory 6G --driver-cores 4 --executor-memory 6G --conf spark.cores.max=4 --conf spark.pyspark.python=/usr/bin/python3 --conf spark.pyspark.driver.python=/usr/bin/python3 /home/hduser/spark/apps/mkt_process_coupons_to_s3.py
        echo "<---End ETL Load Coupons into S3"
else
        echo "--->Begin ETL Load Coupons into S3-$1"
        PYSPARK_PYTHON=/usr/bin/python3 /home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode-ti:7077 --driver-memory 8G --driver-cores 4 --executor-memory 6G --conf spark.cores.max=4 /home/hduser/spark/apps/mkt_process_coupons_to_s3.py -f $1
        echo "<---End ETL Load Coupons into S3-$1"
fi
