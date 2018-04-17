SET SCHEMA atraf;
-- Q17. AVG delay by destination Airport (and compute the standard error):

SELECT "Dest",
       CAST(AVG("ArrDelay") AS DECIMAL(8,2)) AS "AvgDelay",
       CAST(sys.STDDEV_SAMP("ArrDelay") AS DECIMAL(8,2)) AS "AvgDelayErr"
FROM ontime
WHERE "ArrDelay" IS NOT NULL
GROUP BY "Dest"
ORDER BY "AvgDelay", "Dest"
;

