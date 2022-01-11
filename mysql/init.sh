#!/bin/bash
set -euo pipefail

SLEEP=20

HERE=$(dirname $(readlink -f $0))

docker run \
  -v $HERE/init.sql:/init.sql \
  -d --rm --name mysql \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=cabin \
  mysql/mysql-server:8.0

for i in $(seq $SLEEP); do echo -n "."; sleep 1; done
docker exec mysql bash -c "cat /init.sql | mysql -u root -pcabin"
