#!/bin/bash

year=2019

for month in {1..12};
do
	fr=$(date -d "$year/$month/1" +"%Y%m%d")
	to=$(date -d "$year/$month/1 + 1 month - 1 day" +"%Y%m%d")
done