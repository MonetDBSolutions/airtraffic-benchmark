SET SCHEMA atraf;

WITH t1 AS (
    SELECT "Origin", "CRSDepTime", "DepDelay", "Dest", "ArrDelay", "CRSArrTime"
    FROM ontime
    WHERE "DepDelay" > 15 AND "ArrDelay" > 15
      -- we only look at the data of one day
      AND "Month" = 3 AND "DayofMonth" = 24 AND "Year" = 2013
)
SELECT t1."Origin" AS "Airport1",
       CAST(ROUND( SUM(100 * t1."DepDelay") / COUNT(t1."DepDelay") / 100, 2) AS DECIMAL(8,2)) AS "AVGDepDelay",
       CAST(ROUND( SUM(100 * t1."ArrDelay") / COUNT(t1."ArrDelay") / 100, 2) AS DECIMAL(8,2)) AS "AVGArrDelay",
       t2."Origin" AS "Airport2",
       CAST(ROUND( SUM(100 * t2."DepDelay") / COUNT(t2."DepDelay") / 100, 2) AS DECIMAL(8,2)) AS "AVGDepDelay2",
       CAST(ROUND( SUM(100 * t2."ArrDelay") / COUNT(t2."ArrDelay") / 100, 2) AS DECIMAL(8,2)) AS "AVGArrDelay2",
       t2."Dest" AS "Airport3"
FROM t1, t1 AS t2
WHERE t1."Dest" = t2."Origin" AND t1."CRSArrTime" < t2."CRSDepTime"
GROUP BY t1."Origin", t2."Origin", t2."Dest"
-- HAVING AVG(t2."ArrDelay") > 60
ORDER BY "AVGDepDelay" DESC, "AVGArrDelay" DESC, "AVGDepDelay2" DESC, "AVGArrDelay2" DESC, "Airport1", "Airport2", "Airport3"
--LIMIT 30
;

