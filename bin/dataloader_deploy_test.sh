#!/bin/bash

#working dir
day=`date -d "${date}" +%d`
year=`date -d "${date}" +%Y`
month=`date -d "${date}" +%m`

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/runJar

jar="flush-1.0.0-jar-with-dependencies.jar";
testjar="dataloader_flush_fix16tmp_test.jar";

pwd=$(cd "$(dirname "$0")"; pwd)
nowDir=`dirname $pwd`
dist=${nowDir}/target
deployBin=${nowDir}/bin/deployBin





#log deploy info
if [ ! -d ${logDir} ]
then
	mkdir -p ${logDir}
fi

chmod 777 -R ${logDir}
cd ${nowDir}
git pull


#ant
if [ ! -d $dist ]
then
	mkdir -p $dist
fi
cd ${nowDir}
mvn clean
mvn package

#***************
# test jar




#***************
#copy the jar 
#hostliststr="192.168.1.142"
hostliststr="192.168.1.141,192.168.1.142,192.168.1.143,192.168.1.144,192.168.1.145"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host} 
do
	echo ${node}
        echo ${dist}/${jar} ${node}${runJar}/${testjar}
	scp  ${dist}/${jar} ${node}:${runJar}/${testjar}
done




