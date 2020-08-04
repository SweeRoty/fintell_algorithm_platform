#!/bin/bash

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

alter table hgy_02 change imei status int;
alter table hgy_02 change score avg_action_per_device float;
alter table hgy_02 add columns (avg_app_per_device float);
"
nohup beeline -e "$job" &