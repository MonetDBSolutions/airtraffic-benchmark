SET SCHEMA atraf;

SELECT CAST (FLOOR("CRSDepTime"%2400/100) AS INT) AS "Hour",
       "Origin", "Dest", "Carrier", 
       CAST(SUM("DepDel15") AS DOUBLE)/COUNT(*) >= 0.5 AS "PossibleLongDelay"
FROM ontime
GROUP BY "Origin", "Dest", "Carrier", "Hour"
ORDER BY "PossibleLongDelay", "Hour", "Origin", "Dest", "Carrier"
;

