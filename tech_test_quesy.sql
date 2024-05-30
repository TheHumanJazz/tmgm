-- DROP TABLE IF EXISTS output_1;
CREATE TABLE IF NOT EXISTS output_1 (
	id SERIAL PRIMARY KEY,
	dt_report DATE,
	login_hash TEXT,
	server_hash TEXT,
	symbol TEXT,
	currency TEXT,
	sum_volume_prev_7d DOUBLE PRECISION,
	sum_volume_prev_all DOUBLE PRECISION,
	rank_volume_symbol_prev_7d INT,
	rank_count_prev_7d INT,
	sum_volume_2020_08 DOUBLE PRECISION,
	date_first_trade TIMESTAMP,
	row_number INT
);

with dates as (
SELECT GENERATE_SERIES(
    '2020-06-01'::date,
    '2020-09-30'::date,
    '1 day'::interval
) AS date
),
currencies as (
select distinct
	login_hash,
	currency
from users
where enable = 1
)
, trades as (
select
	login_hash,
	ticket_hash,
	server_hash,
	case when symbol = 'USD,CHF' then 'USDCHF' else symbol end as symbol,
	digits,
	cmd,
	volume,
	open_time,
	close_time,
	contractsize
from trades
where volume > 0
)
, filtered_data as (
select
	d.date as dt_report,
	t.login_hash,
	t.server_hash,
	t.symbol,
	c.currency,
	sum(t.volume) as sum_day_volume,
	count(t.ticket_hash) as count_day_trades,
	min(t.close_time) as min_day_close_time
from trades t
inner join currencies c
on t.login_hash = c.login_hash
right join dates d
on DATE_TRUNC('day', t.close_time) = d.date
group by
	dt_report,
	t.login_hash,
	t.server_hash,
	t.symbol,
	c.currency
)
, summed_data as (
select
	dt_report,
	login_hash,
	server_hash,
	symbol,
	currency,
	sum_day_volume,
	count_day_trades,
	min_day_close_time,
	sum(sum_day_volume) over (
		partition by login_hash, server_hash, symbol
		order by dt_report
		range between '6 days' preceding and '0 day' preceding
	) as sum_volume_prev_7d,
	sum(sum_day_volume) over (
		partition by login_hash, server_hash, symbol
		order by dt_report
		rows between unbounded preceding and current row
	) as sum_volume_prev_all,
	sum(count_day_trades) over (
		partition by login_hash, server_hash
		order by dt_report
		range between '6 days' preceding and '0 day' preceding
	) as count_volume_prev_7d,
	sum(
		case
			when dt_report >= date '2020-08-01' and dt_report < date '2020-09-01' then sum_day_volume
			else 0
		end
	) over (
		partition by login_hash, server_hash, symbol
		order by dt_report
		rows between unbounded preceding and current row
	) as sum_volume_2020_08,
	min_day_close_time as date_first_trade
from filtered_data
)
, ranked_data as (
select
	dt_report,
	login_hash,
	server_hash,
	symbol,
	currency,
	sum_volume_prev_7d,
	sum_volume_prev_all,
	dense_rank() over (partition by login_hash, symbol order by sum_volume_prev_7d desc) as rank_volume_symbol_prev_7d,
	dense_rank() over (partition by login_hash order by count_volume_prev_7d desc) as rank_count_prev_7d,
	sum_volume_2020_08,
	date_first_trade,
	row_number() over (order by dt_report, login_hash, server_hash, symbol) as row_number

from summed_data
)
insert into output_1 (
	dt_report,
	login_hash,
	server_hash,
	symbol,
	currency,
	sum_volume_prev_7d,
	sum_volume_prev_all,
	rank_volume_symbol_prev_7d,
	rank_count_prev_7d,
	sum_volume_2020_08,
	date_first_trade,
	row_number
)
select * from ranked_data order by row_number desc;

select * from output_1 order by row_number desc;

