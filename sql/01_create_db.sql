-- Run once as a PostgreSQL superuser (or a role that can CREATE DATABASE).
-- In DBeaver: connect to database "postgres", then execute this script.

CREATE DATABASE crypto_bi
    WITH OWNER = bernardo
         ENCODING = 'UTF8'
         TEMPLATE = template0;

-- After creation, reconnect to crypto_bi and run 02, 03, 04.
