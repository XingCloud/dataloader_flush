#!/bin/bash

#working dir

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/runJar

jar="dataloader_flush_fix16tmp_test.jar";

#hadoopsh="/usr/lib/hadoop/bin/hadoop"

fileencoding="-Dfile.encoding=UTF-8"
verboses="-XX:+HeapDumpOnOutOfMemoryError"
memarg="-server -Xms3g -Xmx3g -Xss256K"
gcarg="-XX:SurvivorRatio=16 -XX:+UseConcMarkSweepGC -XX:NewSize=512M -XX:MaxNewSize=512M -XX:+UseAdaptiveSizePolicy -XX:-ExplicitGCInvokesConcurrent -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=2"

main="com.xingcloud.server.DataLoaderFlush16TmpWather"

hostliststr="dataloader0,dataloader1"
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
    ssh ${node} nohup /usr/java/jdk/bin/java $fileencoding $memarg $gcarg $verboses -classpath ${runJar}/${jar} $main > /dev/null 2>&1 &
    #ssh ${node} nohup ${hadoopsh} jar ${runJar}/${jar} $main   >/dev/null 2>&1 &
done
