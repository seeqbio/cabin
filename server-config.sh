#!/bin/bash

docker run --rm --name mysql -p 3306:3306 -d -e MYSQL_ROOT_PASSWORD=foo mysql/mysql-server:8.0
sleep 4
docker exec mysql mysql -u root -pfoo -Bse "CREATE DATABASE if not exists cabin;CREATE USER if not exists 'cabin'@'%' IDENTIFIED BY 'foo';GRANT ALL PRIVILEGES ON 'cabin'.* TO 'cabin'@'%';FLUSH PRIVILEGES;"
