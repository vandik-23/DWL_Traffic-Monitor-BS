/*
 Q01: Which areas are the most/least frequented. 
 Query finds the average daily traffic counts and adds a ranking column. 
 Then selects the top 3 and bottom 3 traffic ranks for each traffic type.
 */
  
CREATE VIEW q01_avg_daily_traffic AS
WITH traffic_summary AS (
    SELECT
        tt.zst_id,
        tt.traffictype,
        EXTRACT(DAY FROM MAX(tt.datetimefrom) - MIN(tt.datetimefrom)) + 1 AS days_count,
        SUM(tt.total_sum) AS total_traffic
    FROM
        traffic_table tt
    WHERE
        tt.traffictype IN ('Fussgänger', 'Velo')
    GROUP BY
        tt.zst_id, tt.traffictype
),
location_info AS (
    SELECT
        l.zst_id,
        l.sitename,
        l.latitude,
        l.longitude
    FROM
        location l
)
SELECT
    ts.zst_id,
    ts.traffictype,
    li.sitename,
    li.latitude,
    li.longitude,
    ts.total_traffic / ts.days_count AS avg_traffic_per_day,
    traffic_rank
FROM (
    SELECT
        ts.*,
        RANK() OVER (PARTITION BY ts.traffictype ORDER BY ts.total_traffic / ts.days_count DESC) AS traffic_rank,
        RANK() OVER (PARTITION BY ts.traffictype ORDER BY ts.total_traffic / ts.days_count ASC) AS reverse_traffic_rank
    FROM
        traffic_summary ts
) ts
JOIN
    location_info li ON ts.zst_id = li.zst_id
WHERE
    (traffic_rank <= 3 AND traffic_rank > 0) OR (reverse_traffic_rank <= 3 AND reverse_traffic_rank > 0)
ORDER BY
    ts.traffictype, traffic_rank;

----------------------------------   

/* Traffic tracker. This query retrieves the last month of traffic data.
Could be modified to achieve the latest traffic data in order
to create a live tracker.
*/
   
CREATE VIEW traffic_weather_tracker as
SELECT
    tt.datetimeto,
    tt.zst_id,
    l.sitename,
    l.latitude,
    l.longitude,
    tt.traffictype,
    tt.total_sum AS traffic_count,
    w.precipitation,
    w.solarradiation,
    w.airtemperaturehc,
    w.windspeedultrasonic 
FROM
    traffic_table tt
JOIN
    location l ON tt.zst_id = l.zst_id
JOIN
    weather w ON tt.datetimeto = w.timestamp
WHERE
    tt.datetimeto >= CURRENT_DATE - INTERVAL '1 month'
    AND tt.traffictype IN ('Velo', 'Fussgänger')
ORDER BY
    tt.datetimeto DESC;

----------------------------------   

/* Q2: Find fastest growing areas. 
Query compares yearly traffic counts for each location with counts 
of previous year.
*/
   
CREATE VIEW Q02_yoy_traffic_growth AS   
WITH yearly_traffic AS (
    SELECT
        tt.zst_id,
        EXTRACT(YEAR FROM tt.datetimefrom) AS year,
        tt.traffictype,
        SUM(tt.total_sum) AS total_traffic
    FROM
        traffic_table tt
    WHERE
        tt.traffictype IN ('Fussgänger', 'Velo')
    GROUP BY
        tt.zst_id, EXTRACT(YEAR FROM tt.datetimefrom), tt.traffictype
)
SELECT
    l.sitename,
    l.latitude,
    l.longitude,
    yt.year,
    yt.traffictype,
    yt.total_traffic,
    CASE
        WHEN yt.year = MIN(yt.year) OVER (PARTITION BY yt.zst_id, yt.traffictype) THEN NULL  -- Exclude the first year
        ELSE (yt.total_traffic - LAG(yt.total_traffic, 1, 0) OVER (PARTITION BY yt.zst_id, yt.traffictype ORDER BY yt.year)::float) / NULLIF(LAG(yt.total_traffic, 1, 0) OVER (PARTITION BY yt.zst_id, yt.traffictype ORDER BY yt.year)::float, 0) * 100.0
    END AS year_over_year_growth_percent,
    COALESCE(yt.total_traffic - LAG(yt.total_traffic, 1, 0) OVER (PARTITION BY yt.zst_id, yt.traffictype ORDER BY yt.year), 0) AS total_traffic_difference
FROM
    yearly_traffic yt
JOIN
    location l ON yt.zst_id = l.zst_id
ORDER BY
    yt.zst_id, yt.traffictype, yt.year;

   
   
----------------------------------   
/* Q2: Find fastest growing areas. 
Query compares each month traffic counts 
with the same month of the previous year
*/
   
CREATE VIEW Q02_monthly_traffic_growth_view AS
WITH monthly_traffic AS (
    SELECT
        tt.zst_id,
        EXTRACT(YEAR FROM tt.datetimefrom) AS year,
        EXTRACT(MONTH FROM tt.datetimefrom) AS month,
        tt.traffictype,
        SUM(tt.total_sum) AS total_traffic
    FROM
        traffic_table tt
    WHERE
        tt.traffictype IN ('Fussgänger', 'Velo')
    GROUP BY
        tt.zst_id, EXTRACT(YEAR FROM tt.datetimefrom), EXTRACT(MONTH FROM tt.datetimefrom), tt.traffictype
)
SELECT
    l.sitename,
    l.latitude,
    l.longitude,
    mt.year,
    mt.month,
    mt.traffictype,
    mt.total_traffic,
    CASE
        WHEN mt.year = MIN(mt.year) OVER (PARTITION BY mt.zst_id, mt.traffictype) THEN NULL  -- Exclude the first year
        ELSE (mt.total_traffic - LAG(mt.total_traffic, 12, 0) OVER (PARTITION BY mt.zst_id, mt.traffictype ORDER BY mt.year, mt.month)::float) / NULLIF(LAG(mt.total_traffic, 12, 0) OVER (PARTITION BY mt.zst_id, mt.traffictype ORDER BY mt.year, mt.month)::float, 0) * 100.0
    END AS month_on_month_growth_percent,
    COALESCE(mt.total_traffic - LAG(mt.total_traffic, 12, 0) OVER (PARTITION BY mt.zst_id, mt.traffictype ORDER BY mt.year, mt.month), 0) AS total_traffic_difference
FROM
    monthly_traffic mt
JOIN
    location l ON mt.zst_id = l.zst_id
ORDER BY
    mt.zst_id, mt.traffictype, mt.year, mt.month;