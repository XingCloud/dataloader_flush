#!/bin/bash

#working dir

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/runJar

jar="dataloader.jar";

hadoopsh="/usr/lib/hadoop/bin/hadoop"


#***************
#run the dataloader 
memarg="-server -Xms2G -Xmx2G"
gcarg="-XX:SurvivorRatio=4 -XX:+UseConcMarkSweepGC -XX:NewSize=64m -XX:+UseAdaptiveSizePolicy -XX:-ExplicitGCInvokesConcurrent -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=2"
verboses="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintGCApplicationStoppedTime"
main="com.xingcloud.dataloader.DataLoader"
fileencoding="-Dfile.encoding=UTF-8"


hostliststr="10.32.42.4,10.32.42.31,10.32.42.19,10.32.42.17"
#hostliststr="127.0.0.1,localhost"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host} 
do
	echo ${node}
	ssh ${node} ps -aux|grep dataloader.jar|grep com.xingcloud.dataloader.DataLoader|grep -v grep|awk '{print $2}'|xargs kill 
done




