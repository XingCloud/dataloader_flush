#!/bin/bash

#working dir

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/runJar

jar="dataloader_flush_fix16tmp_test.jar";


#***************
main="com.xingcloud.server.DataLoaderFlush16TmpWather"


hostliststr="dataloader0,dataloader1"
#hostliststr="127.0.0.1,localhost"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host} 
do
	echo ${node}
	echo "beforekill"
	ssh ${node} ps aux|grep $main|awk '{print$2}'
	pidlist=`ssh ${node} ps aux|grep $main|awk '{print$2}'`
for pid in $pidlist
do
echo $pid
ssh ${node} kill $pid
done
	echo "afterkill"
	ssh ${node} ps aux|grep $main|awk '{print$2}'
done




