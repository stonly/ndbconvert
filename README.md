NDB-Converter
=============

Convert MyISAM and InnoDB to NDB for MySQL Cluster

Instructions :

python ndb-converter.py

The program will prompt you for the login, password, host, and databases name. 
The resulting output will be the errors and correction attempts before a final "success" or
a ; seperated string of the tables that could not convert.
