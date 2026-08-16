-- ============================================================
-- NYC TAXI DATA ENGINEERING PROJECT
-- SQL ANALYTICS
-- ============================================================


-- ============================================================
-- 1. DAILY PERFORMANCE
-- ============================================================

SELECT
    date,
    total_trips,
    total_distance,
    total_revenue,
    avg_trip_distance,
    avg_trip_duration,
    avg_fare,
    avg_tip
FROM daily_taxi_metrics
ORDER BY date;


-- ============================================================
-- 2. TOP 10 DAYS BY TRIP VOLUME
-- ============================================================

SELECT
    date,
    total_trips
FROM daily_taxi_metrics
ORDER BY total_trips DESC;
LIMIT 10;


-- ============================================================
-- 3. TOP 10 DAYS BY REVENUE
-- ============================================================

SELECT
    date,
    total_revenue
FROM daily_taxi_metrics
ORDER BY total_revenue DESC;
LIMIT 10;


-- ============================================================
-- 4. HOURLY TRIP ANALYSIS
-- ============================================================

SELECT
    hour_of_day,
    total_trips,
    total_distance,
    total_revenue,
    avg_trip_duration,
    avg_fare
FROM hourly_taxi_metrics
ORDER BY hour_of_day;


-- ============================================================
-- 5. PEAK HOURS
-- ============================================================

SELECT
    hour_of_day,
    total_trips,
    total_revenue
FROM hourly_taxi_metrics
ORDER BY total_trips DESC;
LIMIT 5;


-- ============================================================
-- 6. BOROUGH PERFORMANCE
-- ============================================================

SELECT
    PU_Borough,
    total_trips,
    total_distance,
    total_revenue,
    avg_trip_distance,
    avg_trip_duration,
    avg_fare
FROM borough_metrics
ORDER BY total_trips DESC;


-- ============================================================
-- 7. TOP 5 PICKUP BOROUGHS
-- ============================================================

SELECT
    PU_Borough,
    total_trips,
    total_revenue
FROM borough_metrics
ORDER BY total_trips DESC;
LIMIT 5;


-- ============================================================
-- 8. PAYMENT TYPE ANALYSIS
-- ============================================================

SELECT
    payment_type,
    total_trips,
    total_revenue,
    avg_fare,
    avg_tip
FROM payment_metrics
ORDER BY total_revenue DESC;


-- ============================================================
-- 9. REVENUE PER TRIP
-- ============================================================

SELECT
    date,
    total_trips,
    total_revenue,
    ROUND(
        total_revenue / NULLIF(total_trips, 0),
        2
    ) AS revenue_per_trip
FROM daily_taxi_metrics
ORDER BY date;


-- ============================================================
-- 10. HIGHEST REVENUE PER TRIP HOURS
-- ============================================================

SELECT
    hour_of_day,
    total_trips,
    total_revenue,
    ROUND(
        total_revenue / NULLIF(total_trips, 0),
        2
    ) AS revenue_per_trip
FROM hourly_taxi_metrics
ORDER BY revenue_per_trip DESC;


-- ============================================================
-- 11. DAILY RANKING BY REVENUE
-- ============================================================

WITH ranked_days AS
(
    SELECT
        date,
        total_trips,
        total_revenue,
        RANK() OVER (
            ORDER BY total_revenue DESC
        ) AS revenue_rank
    FROM daily_taxi_metrics
)

SELECT
    date,
    total_trips,
    total_revenue,
    revenue_rank
FROM ranked_days
WHERE revenue_rank <= 10
ORDER BY revenue_rank;


-- ============================================================
-- 12. RUNNING REVENUE
-- ============================================================

SELECT
    date,
    total_revenue,

    SUM(total_revenue) OVER (
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS running_revenue

FROM daily_taxi_metrics
ORDER BY date;


-- ============================================================
-- 13. DAILY REVENUE CHANGE
-- ============================================================

SELECT
    date,
    total_revenue,

    LAG(total_revenue) OVER (
        ORDER BY date
    ) AS previous_day_revenue,

    total_revenue -
    LAG(total_revenue) OVER (
        ORDER BY date
    ) AS revenue_change

FROM daily_taxi_metrics
ORDER BY date;


-- ============================================================
-- 14. DAILY TRIP CHANGE
-- ============================================================

SELECT
    date,
    total_trips,

    LAG(total_trips) OVER (
        ORDER BY date
    ) AS previous_day_trips,

    total_trips -
    LAG(total_trips) OVER (
        ORDER BY date
    ) AS trip_change

FROM daily_taxi_metrics
ORDER BY date;


-- ============================================================
-- 15. BEST PERFORMING BOROUGH BY REVENUE
-- ============================================================

SELECT
    PU_Borough,
    total_trips,
    total_revenue,
    avg_fare
FROM borough_metrics
ORDER BY total_revenue DESC;
LIMIT 1;


-- ============================================================
-- 16. SUMMARY
-- ============================================================

SELECT
    COUNT(*) AS total_days,
    SUM(total_trips) AS total_trips,
    SUM(total_distance) AS total_distance,
    SUM(total_revenue) AS total_revenue,
    AVG(avg_trip_distance) AS overall_avg_distance,
    AVG(avg_trip_duration) AS overall_avg_duration,
    AVG(avg_fare) AS overall_avg_fare
FROM daily_taxi_metrics;

