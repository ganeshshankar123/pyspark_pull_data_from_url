#!/bin/bash

set -u

python ingestion.py

if [$? -ne 0]; then
log_error "Failed to execute python script"
fi

echo "Successfully done ingestion!!"
spark-submit --jars aws-java-sdk-1.7.4.jar,hadoop-aws-2.7.3.jar compute.py 200011


if [$? -ne 0]; then
echo "Failed to execute compute script"
fi

echo "Successfully executed workflow!!"