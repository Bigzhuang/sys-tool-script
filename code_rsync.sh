#!/bin/bash

LOCALPATH="/tmp/test/"
REMOTEPATH="/tmp/test/"
REMOTEHOSTS="192.168.6.40 192.168.6.58"

while true;
do 
    for host in $REMOTEHOSTS
        do
            echo "rsync -a $LOCALPATH $host:$REMOTEPATH"
            rsync -avq $LOCALPATH $host:$REMOTEPATH
        done
    sleep 10
done
