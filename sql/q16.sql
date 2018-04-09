SET SCHEMA atraf;

WITH t1 AS ( -- outbound flights of the Major Airports
    SELECT "Origin" AS "Airport", "Month", "DayofMonth", COUNT(*) AS "Count"
    FROM ontime
    WHERE "Origin" = 'ATL' OR "Origin" = 'ORD' OR "Origin" = 'SEA' OR "Origin" = 'STL'
    GROUP BY "Origin", "Month", "DayofMonth"
),
t2 AS ( -- inbound flights of the Major Airports
    SELECT "Dest" AS "Airport", "Month", "DayofMonth", COUNT(*) AS "Count"
    FROM ontime
    WHERE "Dest" = 'ATL' OR "Dest" = 'ORD' OR "Dest" = 'SEA' OR "Dest" = 'STL' 
    GROUP BY "Dest", "Month", "DayofMonth"
),
t3 AS ( -- since each day doesn't appear the same number of times in the database
    SELECT "Month", "DayofMonth", COUNT(*) AS cnt
    FROM (SELECT DISTINCT "Year", "Month", "DayofMonth" FROM ontime) AS t
    GROUP BY "Month", "DayofMonth"
)
-- NB: possibly a bug: no error by "t3.cnt" i.s.o. "AVG(t3.cnt)"
SELECT "Airport", "Month", "DayofMonth", CAST(SUM(t."Count")/AVG(t3.cnt) AS DECIMAL(8,2)) AS "AvgFlightsPerDay"
      -- putting in/out-bound flights together
FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2) AS t LEFT JOIN t3 ON
     (t."Month" = t3."Month" AND t."DayofMonth" = t3."DayofMonth")
GROUP BY "Airport", t."Month", t."DayofMonth"
ORDER BY "Airport", t."Month", t."DayofMonth"
;

