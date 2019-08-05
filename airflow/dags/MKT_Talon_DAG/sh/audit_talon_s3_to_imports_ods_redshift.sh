#!/bin/bash
echo "COMIENZO AUDITORIA TALON LOCAL FILE SYSTEM TO HFDS"
/home/hduser/spark/bin/spark-submit --master local[4] --driver-memory 7G --driver-cores 4 --executor-memory 7G --conf spark.cores.max=4 /home/hduser/airflow-scripts/audit_talon_s3_coupons_to_imports_ods_redshift.py -e $1
