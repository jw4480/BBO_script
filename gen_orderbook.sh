#!/bin/bash

#source ~gdmops/mm/utils.sh

ticker=$1
date=$2
debug=$3
path=$4

if [ -z "$path" ]
then
    orderfile="~/order.$ticker.$date"
    tradefile="~/trade.$ticker.$date"
    path="~"
else
    orderfile="$path/order.$ticker.$date"
    tradefile="$path/trade.$ticker.$date" 
fi


if [[ $debug -eq 1 ]]
then
    perl $path/orderbook.pl --ticker=$ticker --date=$date --tradefile=$tradefile --orderfile=$orderfile --path=$path --debug
else
    perl $path/orderbook.pl --ticker=$ticker --date=$date --tradefile=$tradefile --orderfile=$orderfile --path=$path
fi

