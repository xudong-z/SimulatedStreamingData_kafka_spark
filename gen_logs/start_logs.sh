#!/bin/bash

cd ~/Downloads/gen_logs
lib/genhttplogs.py > logs/access.log &
exit 0
