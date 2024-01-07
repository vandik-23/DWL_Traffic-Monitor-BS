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

-----------------------------------------------------
/* Q:03 Peak times and months. These queries aggregate the data by monthly, daily and hourly averages. */

CREATE VIEW q03_avg_monthly_traffic AS
WITH traffic_summary AS (
    SELECT
        tt.zst_id,
        tt.traffictype,
        TO_CHAR(tt.datetimefrom, 'Month') AS month,
        EXTRACT(MONTH FROM tt.datetimefrom) AS month_number,
        SUM(tt.total_sum) AS total_traffic
    FROM
        traffic_table tt
    WHERE
        tt.traffictype IN ('Fussgänger', 'Velo')
    GROUP BY
        tt.zst_id, tt.traffictype, month, month_number
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
    ts.month,
    ts.month_number,
    li.sitename,
    li.latitude,
    li.longitude,
    AVG(ts.total_traffic) AS avg_traffic_per_month
FROM
    traffic_summary ts
JOIN
    location_info li ON ts.zst_id = li.zst_id
GROUP BY
    ts.zst_id, ts.traffictype, ts.month, ts.month_number, li.sitename, li.latitude, li.longitude
ORDER BY
    ts.traffictype, ts.month_number;

----------------------------------------

CREATE VIEW q03_avg_daily_traffic_weekdays AS
WITH daily_traffic_summary AS (
    SELECT
        t.zst_id,
        t.traffictype,
        date_trunc('day', t.datetimefrom) AS day,
        l.sitename,
        l.latitude,
        l.longitude,
        SUM(t.total_sum) AS total_sum_daily
    FROM
        traffic_table t
        JOIN location l ON t.zst_id = l.zst_id  
    WHERE
        t.traffictype IN ('Velo', 'Fussgänger')
    GROUP BY
        date_trunc('day', t.datetimefrom), t.zst_id, t.traffictype, l.sitename, l.latitude, l.longitude
)
SELECT
    zst_id,
    traffictype,
    EXTRACT(ISODOW FROM day) AS weekday,
    TO_CHAR(day, 'Day') AS weekday_name,
    AVG(total_sum_daily) AS avg_total_weekday_over_years,
    sitename,
    latitude,
    longitude
FROM
    daily_traffic_summary
WHERE
    traffictype IN ('Velo', 'Fussgänger')
GROUP BY
    zst_id, traffictype, weekday, weekday_name, sitename, latitude, longitude
ORDER BY
    weekday, zst_id, traffictype;

-------------------------------------------------

CREATE VIEW q03_avg_hourly_traffic AS
SELECT
  l.sitename,
  l.latitude,
  l.longitude,
  t.zst_id,
  t.traffictype,
  d.hourfrom,
  AVG(t.total_sum) AS avg_total_sum
FROM
  traffic_table t
  JOIN datetime d ON t.datetimefrom = d.datetimefrom
  JOIN location l ON t.zst_id = l.zst_id
WHERE
  t.traffictype IN ('Velo', 'Fussgänger')
GROUP BY
  l.sitename, l.latitude, l.longitude, t.zst_id, t.traffictype, d.hourfrom
ORDER BY
  l.sitename, t.zst_id, t.traffictype, d.hourfrom;


