-- Step 1: Create a date table
WITH date_table AS (
    SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'::interval)::date AS dt_report
),

-- Step 2: Filter enabled users
enabled_users AS (
    SELECT login_hash, server_hash, currency
    FROM users
    WHERE enable = 1
),

-- Step 3: Generate combinations of dt_report, login_hash, server_hash, and symbol
combinations AS (
    SELECT 
        dt_report, 
        login_hash, 
        server_hash, 
        symbol
    FROM date_table
    CROSS JOIN (
        SELECT DISTINCT login_hash, server_hash, symbol
        FROM output_hwl
        WHERE dt_report BETWEEN '2020-06-01' AND '2020-09-30'
    ) AS distinct_combinations
),

-- Step 4: Aggregate data
aggregated_data AS (
    SELECT 
        c.dt_report,
        c.login_hash,
        c.server_hash,
        c.symbol,
        COALESCE(SUM(o.sum_volume_prev_7d), 0) AS sum_volume_prev_7d,
        COALESCE(SUM(o.sum_volume_prev_all), 0) AS sum_volume_prev_all,
        COALESCE(DENSE_RANK() OVER (PARTITION BY c.login_hash, c.symbol ORDER BY SUM(o.sum_volume_prev_7d) DESC), 0) AS rank_volume_symbol_prev_7d,
        COALESCE(DENSE_RANK() OVER (PARTITION BY c.login_hash ORDER BY COUNT(o.id) DESC), 0) AS rank_count_prev_7d,
        COALESCE(SUM(o.sum_volume_2020_08), 0) AS sum_volume_2020_08,
        MIN(o.date_first_trade) AS date_first_trade
    FROM combinations c
    LEFT JOIN output_hwl o ON c.dt_report = o.dt_report 
                            AND c.login_hash = o.login_hash 
                            AND c.server_hash = o.server_hash 
                            AND c.symbol = o.symbol
    GROUP BY c.dt_report, c.login_hash, c.server_hash, c.symbol
)

-- Step 5: Filter by enabled users
SELECT 
    ad.dt_report,
    ad.login_hash,
    ad.server_hash,
    ad.symbol,
    ad.sum_volume_prev_7d,
    ad.sum_volume_prev_all,
    ad.rank_volume_symbol_prev_7d,
    ad.rank_count_prev_7d,
    ad.sum_volume_2020_08,
    ad.date_first_trade
FROM aggregated_data ad
JOIN enabled_users eu ON ad.login_hash = eu.login_hash AND ad.server_hash = eu.server_hash
ORDER BY ad.dt_report, ad.login_hash, ad.server_hash, ad.symbol;
