-- Run as superuser (postgres) to create the langgraph databases
CREATE DATABASE langgraph;
CREATE DATABASE langgraph_test;

-- Optional: create a dedicated role
-- CREATE ROLE langgraph_user WITH LOGIN PASSWORD 'langgraph_pass';
-- GRANT ALL PRIVILEGES ON DATABASE langgraph TO langgraph_user;
-- GRANT ALL PRIVILEGES ON DATABASE langgraph_test TO langgraph_user;
