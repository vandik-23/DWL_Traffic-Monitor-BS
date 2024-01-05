--How does weather data (e.g., temperature, precipitation, wind speed) correlate with fluctuations in pedestrian traffic for retailers?
SELECT weather.*, traffic_table.total_sum, traffic_table.traffictype 
FROM weather
LEFT JOIN traffic_table ON weather.timestamp = traffic_table.datetimeto
where traffic_table.total_sum is not null;

-- 