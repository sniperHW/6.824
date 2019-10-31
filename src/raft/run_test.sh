#!/bin/bash

for((i=1; i<100;i++));
	do
	echo "start " $i	
	go test -race -run 2A;
	if [ $? -eq 0 ]; then
		echo $i "success"
	else
		echo $i "failed"
		break;
	fi
done
