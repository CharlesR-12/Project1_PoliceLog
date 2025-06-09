select * from "Project01_Traffic"
--
--
--
--P_Q-1: What are the top 10 vehicle_Number involved in drug-related stops?
SELECT 
    vehicle_number, COUNT(*) AS drug_stops
FROM "Project01_Traffic"
WHERE drugs_related_stop = TRUE
GROUP BY vehicle_number 
ORDER BY drug_stops DESC
LIMIT 10
--
--
--
--P_Q-2: Which vehicles were most frequently searched?
SELECT 
    vehicle_number, COUNT(*) AS total_searches
FROM "Project01_Traffic"
WHERE search_conducted = TRUE
GROUP BY vehicle_number 
ORDER BY total_searches DESC
--
--
--
--P_Q-3: Driver age group with highest arrest rate?
SELECT 
    CASE 
        WHEN driver_age < 20 THEN 'Under 20'
        WHEN driver_age BETWEEN 21 AND 35 THEN '21-35'
		WHEN driver_age BETWEEN 36 AND 50 THEN '36-50'		
        WHEN driver_age BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent
FROM "Project01_Traffic"
GROUP BY age_group 
ORDER BY arrest_rate_percent DESC
--
--
--
--P_Q-4: Gender distribution of drivers stopped in each country?
select driver_gender, country_name, count (*) AS drivers_stopped 
FROM "Project01_Traffic" 
GROUP BY driver_gender, country_name 
ORDER BY driver_gender, country_name
--
--
--
--P_Q-5: Race and gender combination with highest search rate?
select driver_gender, driver_race, count (*) AS drivers_stopped 
FROM "Project01_Traffic"
GROUP BY driver_gender, driver_race 
ORDER BY driver_gender, driver_race
--
--
--
--P_Q-6: Time of day with most traffic stops?
SELECT stop_time AS hour_of_day, COUNT(*) AS Most_Traffice_Stop 
FROM "Project01_Traffic" 
GROUP BY hour_of_day 
ORDER BY hour_of_day, Most_Traffice_Stop DESC;
--
--
--
--P_Q-7: Average stop duration for different violations?
SELECT violation_raw, AVG(
           CASE stop_duration
               WHEN '0-15 Min' THEN 5
               WHEN '16-30 Min' THEN 10
               WHEN '30+ Min' THEN 20
           END
       ) AS avg_stop_duration_estimate
FROM "Project01_Traffic"
GROUP BY violation_raw
ORDER BY avg_stop_duration_estimate DESC
--
--
--
--P_Q-8: Are stops during night more likely to lead to arrests?
SELECT 
    CASE 
        WHEN stop_time BETWEEN '04:00:00' AND '11:59:59' THEN 'Morning'
        WHEN stop_time BETWEEN '12:00:00' AND '16:59:59' THEN 'Afternoon'
        WHEN stop_time BETWEEN '17:00:00' AND '21:59:59' THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent
FROM "Project01_Traffic"
GROUP BY time_of_day
ORDER BY total_stops, arrest_rate_percent, total_arrests DESC
--
--
--
--P_Q-9: Violations most associated with searches or arrests?
SELECT 
    violation,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS searches,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrests,
    ROUND(SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate_percent,
    ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent
FROM "Project01_Traffic"
GROUP BY violation 
ORDER BY search_rate_percent DESC, arrest_rate_percent DESC
--
--
--
--P_Q-10: Violations most common among younger drivers (<25)?
SELECT violation, COUNT(*) AS total_stops_under_25 
FROM "Project01_Traffic" 
WHERE driver_age < 25 
GROUP BY violation ORDER BY total_stops_under_25 DESC
--
--
--
--P_Q-11: Violation rarely resulting in search or arrest?
SELECT 
    violation,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate_percent,
    ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent
FROM "Project01_Traffic"
GROUP BY violation
ORDER BY search_rate_percent DESC
--
--
--
--P_Q-12: Countries with highest rate of drug-related stops?
SELECT
    country_name,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) AS drug_stops,
    ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS drug_stop_rate_percent
FROM "Project01_Traffic"
GROUP BY country_name
ORDER BY drug_stop_rate_percent DESC
--
--
--
--P_Q-13: Arrest rate by country and violation?
SELECT
    country_name, violation,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent
FROM "Project01_Traffic"
GROUP BY country_name, violation
ORDER BY arrest_rate_percent DESC, total_stops DESC
--
--
--
--P_Q-14: Country with most stops with search conducted?
SELECT 
    country_name, COUNT(*) AS total_search_stops 
FROM "Project01_Traffic"
WHERE search_conducted = TRUE 
GROUP BY country_name ORDER BY total_search_stops DESC


-- (Complex Question):
-- 1.Yearly Breakdown of Stops and Arrests by Country (Using Subquery and Window Functions)
WITH yearly_data AS (
    SELECT
        country_name,
        EXTRACT(YEAR FROM stop_date::date) AS year,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests
    FROM "Project01_Traffic"
    GROUP BY country_name, EXTRACT(YEAR FROM stop_date::date)
)
SELECT
    country_name,
    year,
    total_stops,
    total_arrests,
    ROUND((total_arrests::decimal / NULLIF(total_stops, 0)) * 100, 2) AS arrest_rate_percent,
    SUM(total_stops) OVER (PARTITION BY country_name ORDER BY year) AS cumulative_stops,
    SUM(total_arrests) OVER (PARTITION BY country_name ORDER BY year) AS cumulative_arrests
FROM yearly_data
ORDER BY country_name, year


-- 2.Driver Violation Trends Based on Age and Race (Join with Subquery)
WITH violation_summary AS (
    SELECT
        driver_age,
        driver_race,
        violation,
        COUNT(*) AS total_stops
    FROM "Project01_Traffic"
    GROUP BY driver_age, driver_race, violation
)
SELECT
    v.driver_age,
    v.driver_race,
    v.violation,
    v.total_stops,
    ROUND(
        v.total_stops::decimal / NULLIF(t.total_stops_by_age_race, 0) * 100, 2
    ) AS stop_percentage_within_group
FROM violation_summary v
JOIN (
    SELECT
        driver_age,
        driver_race,
        COUNT(*) AS total_stops_by_age_race
    FROM "Project01_Traffic"
    GROUP BY driver_age, driver_race
) t
ON v.driver_age = t.driver_age AND v.driver_race = t.driver_race
ORDER BY v.driver_race, v.driver_age, v.total_stops DESC


-- 3.Time Period Analysis of Stops (Joining with Date Functions) , Number of Stops by Year,Month, Hour of the Day
SELECT
    EXTRACT(YEAR FROM stop_date::date) AS year,
    EXTRACT(MONTH FROM stop_date::date) AS month,
    EXTRACT(HOUR FROM stop_time::time) AS hour,
    COUNT(*) AS total_stops
FROM "Project01_Traffic"
GROUP BY
    EXTRACT(YEAR FROM stop_date::date),
    EXTRACT(MONTH FROM stop_date::date),
    EXTRACT(HOUR FROM stop_time::time)
ORDER BY year, month, hour


-- 4.Violations with High Search and Arrest Rates (Window Function)
WITH violation_stats AS (
    SELECT
        violation,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
        SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests
    FROM "Project01_Traffic"
    GROUP BY violation
)

SELECT
    violation,
    total_stops,
    total_searches,
    total_arrests,
    ROUND((total_searches::decimal / NULLIF(total_stops, 0)) * 100, 2) AS search_rate_percent,
    ROUND((total_arrests::decimal / NULLIF(total_stops, 0)) * 100, 2) AS arrest_rate_percent,

    -- Rank violations by search rate (highest first)
    RANK() OVER (ORDER BY (total_searches::decimal / NULLIF(total_stops, 0)) DESC) AS search_rate_rank,

    -- Rank violations by arrest rate (highest first)
    RANK() OVER (ORDER BY (total_arrests::decimal / NULLIF(total_stops, 0)) DESC) AS arrest_rate_rank

FROM violation_stats
ORDER BY search_rate_rank, arrest_rate_rank


-- 5.Driver Demographics by Country (Age, Gender, and Race)
SELECT
    country_name,
    driver_gender,
    driver_race,
    driver_age,
    COUNT(*) AS total_stops
FROM "Project01_Traffic"
GROUP BY
    country_name,
    driver_gender,
    driver_race,
    driver_age
ORDER BY
    country_name, driver_gender, driver_race, driver_age


-- 6.Top 5 Violations with Highest Arrest Rates
SELECT
    violation,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND((SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END)::decimal / NULLIF(COUNT(*), 0)) * 100, 2) AS arrest_rate_percent
FROM "Project01_Traffic"
GROUP BY violation
ORDER BY arrest_rate_percent DESC
LIMIT 5