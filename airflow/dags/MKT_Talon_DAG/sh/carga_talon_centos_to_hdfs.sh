#!/bin/bash
if [ -z "$4" ]
then
	echo "Begin ETL Extract carga_talon_centos_to_hdfs"
	/home/hduser/spark/bin/spark-submit --master local[4]  --driver-memory 7G --driver-cores 4 --executor-memory 7G --conf spark.cores.max=4 /home/hduser/spark/apps/carga_talon_centos_to_hdfs.py -e $1 -r $2 -t $3
	echo "End ETL Extract carga_talon_centos_to_hdfs"

else
	 echo "Begin ETL Extract carga_talon_centos_to_hdfs"
        /home/hduser/spark/bin/spark-submit --master local[4]  --driver-memory 7G --driver-cores 4 --executor-memory 7G --conf spark.cores.max=4 /home/hduser/spark/apps/carga_talon_centos_to_hdfs.py -p $1 -e $2 -r $3 -t $4 -o $5
        echo "End ETL Extract carga_talon_centos_to_hdfs"

fi
