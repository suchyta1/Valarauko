#!/bin/bash

if [ -n "$2" ]
then
	source $2
fi
./GenerateJob.py $1 $2


