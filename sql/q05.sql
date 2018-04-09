SET SCHEMA atraf;

WITH t AS ( -- #long_delays/year
    SELECT "Year", COUNT(*) AS c1 
    FROM ontime 
    WHERE "DepDelay" > 15
    GROUP BY "Year"
), 
t2 AS (-- #delays/year
    SELECT "Year", COUNT(*) AS c2 
    FROM ontime 
    GROUP BY "Year"
) 
SELECT t."Year", CAST(c1 AS DECIMAL(16,2))/c2*100 AS "ProportionLongDelay"
FROM t JOIN t2 ON (t."Year" = t2."Year")
ORDER BY t."Year"
;

