#!/bin/bash

java -jar client.jar localhost 51000 jobs.jar jobs.Hello &
java -jar client.jar localhost 51000 jobs.jar jobs.Mvm &
java -jar client.jar localhost 51000 jobs.jar jobs.Hello &
java -jar client.jar localhost 51000 jobs.jar jobs.Mvm &
java -jar client.jar localhost 51000 jobs.jar jobs.Hello &
