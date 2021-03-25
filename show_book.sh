#!/bin/bash
server=$1
symbol=$2
book=$3


/home/gdmops/mm/bin/gen_orderbook.sh $server $symbol |awk -v b="$book" '$5~b'

exit 0
