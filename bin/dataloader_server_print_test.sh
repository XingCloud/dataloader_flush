#!/bin/bash

#working dir

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/runJar


#***************
#run the dataloader 
main="com.xingcloud.server.DataLoaderFlush16TmpWather"


hostliststr="192.168.1.142,192.168.1.143,192.168.1.144,192.168.1.145"
#hostliststr="127.0.0.1,localhost"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host} 
do
	echo ${node}
	ssh ${node} ps aux|grep $main|awk '{print$2}'
done




