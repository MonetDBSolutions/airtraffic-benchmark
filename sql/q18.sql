SET SCHEMA atraf;

SELECT "Carrier", "Year", COUNT(*) 
FROM ontime
GROUP BY "Carrier", "Year"
ORDER BY "Carrier", "Year"
;

