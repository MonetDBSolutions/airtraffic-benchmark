SET SCHEMA atraf;

SELECT "DayOfWeek", COUNT(*) AS "Flights"
FROM ontime 
WHERE "DepDelay" > 15
GROUP BY "DayOfWeek"
ORDER BY "DayOfWeek"
;

