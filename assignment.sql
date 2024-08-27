WITH active_users AS (
    SELECT login_hash, server_hash, currency
    FROM Users
    WHERE enable = 1
),
report_dates AS (
    SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'::interval)::date AS dt_report
),
user_trades AS (
    SELECT
        t.login_hash,
        t.server_hash,
        t.symbol,
        t.volume,
        t.close_time::date AS trade_date,
        t.contractsize,
        CASE
            -- Volume for trades in August 2020
            WHEN t.close_time::date BETWEEN '2020-08-01' AND '2020-08-31' THEN t.volume
            ELSE 0
        END AS august_volume,
        t.close_time
    FROM Trades t
    JOIN active_users au ON t.login_hash = au.login_hash AND t.server_hash = au.server_hash
),
aggregated_data AS (
    SELECT
        rd.dt_report,
        au.login_hash,
        au.server_hash,
        ut.symbol,
        au.currency,
        COALESCE(SUM(CASE 
                        WHEN ut.trade_date BETWEEN rd.dt_report - INTERVAL '6 days' AND rd.dt_report THEN ut.volume 
                        ELSE 0 
                     END), 0) AS sum_volume_prev_7d,
        COALESCE(SUM(ut.volume), 0) AS sum_volume_prev_all,
        DENSE_RANK() OVER (PARTITION BY au.login_hash, ut.symbol ORDER BY SUM(CASE 
                        WHEN ut.trade_date BETWEEN rd.dt_report - INTERVAL '6 days' AND rd.dt_report THEN ut.volume 
                        ELSE 0 
                     END) DESC) AS rank_volume_symbol_prev_7d,
        DENSE_RANK() OVER (PARTITION BY au.login_hash ORDER BY COUNT(CASE 
                        WHEN ut.trade_date BETWEEN rd.dt_report - INTERVAL '6 days' AND rd.dt_report THEN 1 
                        ELSE NULL 
                     END) DESC) AS rank_count_prev_7d,
        COALESCE(SUM(CASE 
                        -- Accumulate volume for trades in August 2020, up to the current dt_report
                        WHEN ut.trade_date BETWEEN '2020-08-01' AND rd.dt_report THEN ut.august_volume 
                        ELSE 0 
                     END), 0) AS sum_volume_2020_08,
        MIN(ut.close_time) AS date_first_trade
    FROM
        active_users au
    CROSS JOIN report_dates rd
    LEFT JOIN user_trades ut
    ON au.login_hash = ut.login_hash AND au.server_hash = ut.server_hash AND ut.trade_date <= rd.dt_report
    GROUP BY rd.dt_report, au.login_hash, au.server_hash, ut.symbol, au.currency
),
final_output AS (
    SELECT
        ROW_NUMBER() OVER () AS id,
        dt_report,
        login_hash,
        server_hash,
        COALESCE(symbol, '') AS symbol,
        currency,
        sum_volume_prev_7d,
        sum_volume_prev_all,
        rank_volume_symbol_prev_7d,
        rank_count_prev_7d,
        sum_volume_2020_08,
        date_first_trade
    FROM aggregated_data
)
SELECT * FROM final_output
ORDER BY dt_report, login_hash, server_hash, symbol;
