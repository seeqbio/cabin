#Cabin

This repo contains Cabin, a tool to facilitate the versioning of database tables.


## Dev Environment Setup

To get your Cabin dev environment up and running, do the following:

1) Clone the Cabin repo
2) Run the official MySQL Docker image, passing it a root user password of your choice: `docker run --name mysql -it -p 3306:3306 -e MYSQL_ROOT_PASSWORD=<password> mysql/mysql-server:8.0`
3) Enter the container: `docker exec -it mysql bash`
4) Once inside the container, log into the mysql server: `mysql -u root -p <password>`
5) Create DB: `create database cabin;`
6) Create user: `CREATE USER 'cabin'@'%' IDENTIFIED BY <password>;`
7) Grant privileges to user: `GRANT ALL PRIVILEGES ON `cabin`.* TO "cabin"@"%";`
8) Flush privileges: `FLUSH PRIVILEGES;`
