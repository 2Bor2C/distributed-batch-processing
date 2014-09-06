#!/bin/bash

for i in {1..8}
do
java -jar worker.jar localhost 52010 52010+i &
done

./job1.sh
