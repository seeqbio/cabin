CREATE DATABASE IF NOT EXISTS cabin;
CREATE USER IF NOT EXISTS 'cabin'@'%' IDENTIFIED BY 'cabin';
GRANT ALL PRIVILEGES ON cabin.* TO 'cabin'@'%';
FLUSH PRIVILEGES;
