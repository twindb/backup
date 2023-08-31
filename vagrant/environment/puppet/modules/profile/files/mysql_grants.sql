CREATE USER 'dba'@'%' IDENTIFIED WITH mysql_native_password BY 'qwerty';
CREATE USER 'dba'@'localhost' IDENTIFIED WITH mysql_native_password BY 'qwerty';
CREATE USER 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'qwerty';

GRANT ALL ON *.* TO 'dba'@'%' WITH GRANT OPTION;
GRANT ALL ON *.* TO 'dba'@'localhost' WITH GRANT OPTION;
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
