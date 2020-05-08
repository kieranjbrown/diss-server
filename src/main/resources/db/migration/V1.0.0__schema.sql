create schema if not exists reporting;
--
 --MY IMPLEMENTATION H2 SCHEMA
--  create table if not exists reporting.trade_data
--  (
--  id UUID not null,
--  stock varchar(5) not null,
--  price decimal(5,2) not null,
--  volume int not null,
--  valid_time_start date not null,
--  valid_time_end date not null,
--  system_time_start timestamp(6),
--  system_time_end timestamp(6),
--  buy_sell_flag char(1) not null,
--  market_limit_flag char(1) not null
--  );
--
--  alter table reporting.trade_data alter column system_time_start set default CURRENT_TIMESTAMP;

-- MY IMPLEMENTATION MARIADB SCHEMA
-- create table if not exists reporting.trade_data
-- (
-- id varchar(128) not null,
-- stock varchar(5) not null,
-- price decimal(5,2) not null,
-- volume int not null,
-- valid_time_start date not null,
-- valid_time_end date not null,
-- system_time_start timestamp(6),
-- system_time_end timestamp(6),
-- buy_sell_flag char(1) not null,
-- market_limit_flag char(1) not null,
-- constraint pk primary key (id, valid_time_start, valid_time_end)
-- );
-- alter table reporting.trade_data alter column system_time_start set default CURRENT_TIMESTAMP;

-- --
-- -- -- MARIADB IMPLEMENTATION SCHEMA
create table if not exists reporting.trade_data
(
id varchar(128) primary key not null,
stock varchar(5) not null,
price decimal(5,2) not null,
volume int not null,
valid_time_start date not null,
valid_time_end date not null,
system_time_start timestamp(6) AS ROW START,
system_time_end timestamp(6) AS ROW END,
buy_sell_flag char(1) not null,
market_limit_flag char(1) not null,
PERIOD FOR application_time(valid_time_start, valid_time_end),
PERIOD FOR system_time(system_time_start, system_time_end)
)
WITH SYSTEM VERSIONING;