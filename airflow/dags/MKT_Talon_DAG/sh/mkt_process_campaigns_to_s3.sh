#!/bin/bash
if [ -z "$1" ]
then
        echo "--->Begin BATCH MKT Campaigns"
        /home/hduser/spark/bin/spark-submit --master local[4] --driver-memory 6G --driver-cores 4 --executor-memory 6G --conf spark.cores.max=4 --conf spark.pyspark.python=/usr/bin/python3 --conf spark.pyspark.driver.python=/usr/bin/python3 /home/hduser/spark/apps/mkt_process_campaigns_to_s3.py
        echo "<---End BATCH MKT Campaigns"
else
        echo "--->Begin BATCH MKT Campaigns-$1"
        /home/hduser/spark/bin/spark-submit --master spark://hadoop-namenode-ti:7077 --driver-memory 8G --driver-cores 4 --executor-memory 6G --conf spark.cores.max=4 /home/hduser/spark/apps/mkt_process_campaigns_to_s3.py -f $1
        echo "<---End BATCH MKT Campaigns-$1"
fi
