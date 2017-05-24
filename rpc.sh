#!/bin/bash
HOSTS="192.168.6.40 192.168.6.41 192.168.6.58"
if [ -z $1 ];then
    echo "need input command you want to remote excute"
    exit 1
fi
for HOST in $HOSTS;
    do echo "###################################"
    echo "EXECUTE $1 ON SERVER $HOST:"; 
    echo "###################################"
    ssh $HOST "$1";
done
