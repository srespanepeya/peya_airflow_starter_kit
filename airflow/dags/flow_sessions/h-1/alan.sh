#!/bin/bash
if [ -z "$5" ]
then
        echo "Begin ETL Alan "$1$2
        /home/hduser/spark/bin/spark-submit --master local[4] --driver-memory 7G --driver-cores 4 --executor-memory 7G --conf spark.cores.max=4 /home/hduser/spark/apps/alan/alan.py -s $1 -t $2 -p $3 -r $4
        echo "End ETL Alan "$1$2
else
        echo "Begin ETL Alan"$1$2
	/home/hduser/spark/bin/spark-submit --master local[4] --driver-memory 7G --driver-cores 4 --executor-memory 7G --conf spark.cores.max=4 /home/hduser/spark/apps/alan/alan.py -s $1 -t $2 -p $3 -r $4 -f $5
	
        #/home/hduser/spark/bin/spark-submit --master local[4] --driver-memory 7G --driver-cores 4 --executor-memory 7G --conf spark.cores.max=4 --conf spark.pyspark.python=/usr/bin/python3 --conf spark.pyspark.driver.python=/usr/bin/python3 /home/hduser/spark/apps/alan_diego/alan.py -s $1 -t $2 -f $3
        echo "End ETL Alan"$1$2
fi
