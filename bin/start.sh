#!/bin/sh
nohup java -Dfileaccess=mem-map -jar ./target/poseidon-db-1.0-SNAPSHOT.jar /tmp/data >/dev/null 2>&1 &
