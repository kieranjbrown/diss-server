create schema if not exists reporting;

create table if not exists reporting.trade_data
(
trade_id UUID default random_uuid() not null,
version int not null,
stock varchar(5) not null,
price decimal(5) not null,
volume int not null,
valid_time_start datetime(6) not null,
valid_time_end datetime(6) not null default '9999-12-31 00:00:00.000000',
system_time_start datetime(6) not null default systime(),
system_time_end datetime(6) not null default '9999-12-31 00:00:00.000000',
buy_sell_flag char(1) not null,
market_limit_flag char(1) not null
);

alter table reporting.trade_data add primary key (trade_id, version);