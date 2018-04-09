SET SCHEMA atraf;
-- See schema.sql for the definition of "tmp" table

WITH t1 AS (
    SELECT "Carrier", CAST (FLOOR("CRSDepTime"%2400/100) AS INT) AS "Hour", 
           CAST(AVG("ArrDelay") AS DECIMAL(8,2)) AS "PredictedArrDelay"
    FROM ontime
    WHERE "Year" = 2007 AND "Month" = 10 AND "DayofMonth" = 24
    GROUP BY "Carrier", "Hour"
),
t2 AS (
    SELECT t."Carrier", tmp.*
    FROM tmp, (SELECT DISTINCT "Carrier" FROM t1) AS t
)
SELECT "Carrier", "Hour", SUM("PredictedArrDelay")
FROM (SELECT * FROM t1 UNION SELECT * FROM t2) AS t
GROUP BY "Carrier", "Hour"
ORDER BY "Carrier", "Hour"
;

