#!/bin/bash

#working dir

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/runJar


#***************
#run the dataloader 
main="com.xingcloud.server.DataLoaderFlush16TmpWather"


hostliststr="dataloader0,dataloader1"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host} 
do
	echo ${node}
	ssh ${node} ps aux|grep $main|awk '{print$2}'
done




