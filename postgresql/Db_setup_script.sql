
--Add the read-only user to view pg_config

CREATE ROLE info_ro WITH LOGIN PASSWORD '*********';

GRANT SELECT ON PG_SETTINGS TO info_ro;



--Show the most common PS param values:

--we use this in conjunction with <sql> marco in confluence, ex.
{sql:dataSource=bbpdbsrv01|border=3|showsql=true}
select  name, setting, unit, source, category 
from
pg_settings
where name in 
('listen_addresses','max_connections','shared_buffers', 'effective_cache_size','checkpoint_segments','checkpoint_timeout','checkpoint_completion_target',
'autovacuum','log_min_error_statement','log_min_duration_statement','log_statement','default_statistics_target','work_mem','maintenance_work_mem','wal_sync_method',
'wal_buffers','constraint_exclusion','synchronous_commit','synchronous_standby_names','default_statitics_target','fsync')
or
source not in ('default','override');
{sql}


----------------------------------------------------------------------------------------------------
--Creating of new Database 'new_db' with correct setting

--As superuser, create the owner and database
create role new_db with login password 'ffffff';
alter role new_db CREATEROLE;
create database new_db owner = new_db;

--As superuser, revoke access to new database
revoke connect on database new_db from public;
--go to new_db, and revoke all privileges on its public scheman
\c new_db
revoke all on schema public from public;
grant all on schema public to new_db;

--As 'new_db' owner, create the read-only user
create role new_db_ro with login password 'gggggg';

--grant proper privilege
grant connect on database new_db to new_db_ro;
grant usage on schema public to new_db_ro;
grant select on all tables in schema public to new_db_ro;
commit;


----------------------------------------------------------------------------------------------------



