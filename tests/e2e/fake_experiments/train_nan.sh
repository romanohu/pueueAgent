#!/usr/bin/env bash
echo "step 1 loss 0.9"
echo "step 2 loss NaN"
sleep 300 # NaN を出したまま走り続ける(sentinel が検知して介入する状況)
