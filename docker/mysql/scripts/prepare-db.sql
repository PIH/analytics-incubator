drop database if exists openmrs;
create database humci default charset utf8;
create user 'petldbadmin';
grant all privileges on humci to petldbadmin;
