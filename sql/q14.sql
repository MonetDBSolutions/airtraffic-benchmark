SET SCHEMA atraf;

WITH t1 AS ( -- median of outbound delays
    SELECT "Year", "Origin" AS "Airport", sys.MEDIAN("DepDelay") AS "MedianOutboundDelay"
    FROM ontime 
    WHERE "DepDelay" > 15
    GROUP BY "Year", "Origin"
),
t2 AS ( -- median of inbound delays
    SELECT "Year", "Dest" AS "Airport", sys.MEDIAN("ArrDelay") AS "MedianInboundDelay"
    FROM ontime 
    WHERE "DepDelay" > 15
    GROUP BY "Year", "Dest"
),
t3 AS ( -- merge t1, t2 into one table
    SELECT t1."Year" AS y1, t1."Airport" AS a1, "MedianOutboundDelay",
           t2."Year" AS y2, t2."Airport" AS a2, "MedianInboundDelay"
    FROM t1 FULL OUTER JOIN t2 ON
        (t1."Year" = t2. "Year" AND t1."Airport" = t2."Airport")
)
SELECT CASE WHEN y1 IS NULL THEN y2 ELSE y1 END AS "Year",
       CASE WHEN a1 IS NULL THEN a2 ELSE a1 END AS "Airport",
       "MedianOutboundDelay", "MedianInboundDelay"
FROM t3
ORDER BY "Year", "Airport"
;

