SET SCHEMA atraf;

WITH t1 AS ( -- #flights per route before 9/11
    SELECT SQL_MIN("Origin", "Dest") || ' <-> ' ||
           SQL_MAX("Origin", "Dest") AS route, 
           COUNT(*) AS cnt_before 
    FROM ontime
    WHERE '2010-09-11' < "FlightDate" AND "FlightDate" < '2011-09-11'
    GROUP BY route
),
t2 AS ( -- #flights per route after 8/11
    SELECT SQL_MIN("Origin", "Dest") || ' <-> ' ||
           SQL_MAX("Origin", "Dest") AS route, 
           COUNT(*) AS cnt_after 
    FROM ontime
    WHERE '2011-09-11' <= "FlightDate" AND "FlightDate" < '2012-09-11'
    GROUP BY route
),
t3 AS ( -- merge t1, t2 into one table
    SELECT t1.route AS route1, t1.cnt_before, t2.route AS route2, t2.cnt_after
    FROM t1 FULL OUTER JOIN t2 ON (t1.route = t2.route)
)
SELECT CASE WHEN route1 IS NULL THEN route2 ELSE route1 END AS "Route",
       cnt_before AS "FlightsBefore", cnt_after AS "FlightsAfter"
FROM t3
ORDER BY "Route"
;
