SET SCHEMA atraf;

SELECT "Dest", CAST(SUM("DepDel15") AS DECIMAL(16,2))/COUNT(*)*100 AS "ProportionLongDelay"
FROM ontime
WHERE "Origin" = 'ORD' AND "Year" = 2013
GROUP BY "Dest"
ORDER BY "ProportionLongDelay" DESC, "Dest"
;

