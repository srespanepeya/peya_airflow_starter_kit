#!/bin/sh
cd /home/hduser/backendbi-audit/HueTables
ROOT_PATH=`pwd`
java -Xms1G -Xmx2G -jar MvnHueAudit.jar prod
