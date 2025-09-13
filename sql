CREATE DATABASE etl_db;
CREATE USER 'airflow'@'%' IDENTIFIED BY 'airflow';
Alter user 'airflow'@'%' IDENTIFIED WITH mysql_native_password BY 'airflow';
GRANT ALL PRIVILEGES ON etl_db.* TO 'airflow'@'%';
FLUSH PRIVILEGES;
