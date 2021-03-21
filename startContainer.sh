#!/bin/bash

mvn clean package
python log_generator.py --start-date 12.12.2020/09:10:10 --end-date 13.12.2020/09:10:10 --period 10 --num-files 7 --devices 15 --malformed 2 --output-dir ./data
docker run -p 50070:50070 -d --name hadoop hadoop-single-node:latest
docker cp ./target/MapRed-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop:root
docker cp data hadoop:root
docker exec -it hadoop /bin/bash
