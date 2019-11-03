#!/bin/bash

for((i=1; i<1000;i++));
	do
	echo "start " $i	
	go test -race -run 2C;
	if [ $? -eq 0 ]; then
		echo $i "success"
	else
		echo $i "failed"
		break;
	fi
done
