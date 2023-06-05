# !/bin/bash
ray start --address=$1
tail -f /dev/null