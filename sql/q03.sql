SET SCHEMA atraf;

SELECT "Year", "Month", COUNT(*) AS "Flights"
FROM ontime 
GROUP BY "Year", "Month"
ORDER BY "Year", "Month"
;

