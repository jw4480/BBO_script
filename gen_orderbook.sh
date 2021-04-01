#!/bin/bash

#source ~gdmops/mm/utils.sh

ticker=$1
orderfile="/home/justin/BBO_script/$ticker.order"
tradefile="/home/justin/BBO_script/$ticker.trade"

#cat $orderfile | perl /home/justin/BBO_script/gen_simple.pl --ticker=$ticker --tradefile=$tradefile
perl /home/justin/BBO_script/orderbook.pl --tradefile=$tradefile --orderfile=$orderfile --debug
