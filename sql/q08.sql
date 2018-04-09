SET SCHEMA atraf;
-- Q08. Arrival Delay by Departure Hour: Weekdays and Weekends .

WITH t1 AS(
    SELECT 'Weekend' AS "DayOfWeek", CAST (FLOOR("CRSDepTime"%2400/100) AS INT) AS "Hour",
           CAST(AVG("ArrDelay") AS DECIMAL(8,2)) AS "ArrDelay",
           CAST(AVG("ArrDelay") - 2 * sys.STDDEV_SAMP("ArrDelay") AS DECIMAL(8,2)) AS "lowerConfBound",
           CAST(AVG("ArrDelay") + 2 * sys.STDDEV_SAMP("ArrDelay") AS DECIMAL(8,2)) AS "upperConfBound"
    FROM ontime
    WHERE "DayOfWeek" in (6, 7) -- Saturday or Sunday
    GROUP BY "Hour"
),
t2 AS(
    SELECT 'Weekday' AS "DayOfWeek", CAST (FLOOR("CRSDepTime"%2400/100) AS INT) AS "Hour",
           CAST(AVG("ArrDelay") AS DECIMAL(8,2)) AS "ArrDelay",
           CAST(AVG("ArrDelay") - 2 * sys.STDDEV_SAMP("ArrDelay") AS DECIMAL(8,2)) AS "lowerConfBound",
           CAST(AVG("ArrDelay") + 2 * sys.STDDEV_SAMP("ArrDelay") AS DECIMAL(8,2)) AS "upperConfBound"
    FROM ontime
    WHERE "DayOfWeek" in (1,2,3,4,5) -- Monday to Friday
    GROUP BY "Hour"
)
SELECT * FROM t1 UNION ALL SELECT * FROM t2
ORDER BY "DayOfWeek", "Hour"
;

