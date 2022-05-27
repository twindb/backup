CREATE USER 'dba'@'%' IDENTIFIED BY 'qwerty';
CREATE USER 'dba'@'localhost' IDENTIFIED BY 'qwerty';
CREATE USER 'repl'@'%' IDENTIFIED BY 'qwerty';

GRANT ALL ON *.* TO 'dba'@'%' WITH GRANT OPTION;
GRANT ALL ON *.* TO 'dba'@'localhost' WITH GRANT OPTION;
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
