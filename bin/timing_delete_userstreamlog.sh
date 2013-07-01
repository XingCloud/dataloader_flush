#!/bin/bash
day=`date -d "10 days ago" +%Y-%m-%d`
rm /data/log/*.log.${day}
