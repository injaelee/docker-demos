CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- create the 'rxcliodev' database
--
CREATE DATABASE rxcliodev;
\c rxcliodev;


-- create schema 'clio'
--
CREATE SCHEMA IF NOT EXISTS clio;

-- create the clio app user
--
CREATE USER clio_rpld_bot WITH ENCRYPTED PASSWORD 'cLiO#b0t';
GRANT USAGE ON SCHEMA clio TO clio_rpld_bot;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA clio TO clio_rpld_bot;
