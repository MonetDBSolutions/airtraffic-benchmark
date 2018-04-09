SET SCHEMA atraf;

WITH t1 AS ( -- flights to ORD
    SELECT "Origin" AS ap, COUNT(*) AS cnt_in
    FROM ontime
    WHERE "Dest" = 'ORD'
    GROUP BY "Origin"
),
t2 AS ( -- flights from ORD
    SELECT "Dest" AS ap, COUNT(*) AS cnt_out
    FROM ontime
    WHERE "Origin" = 'ORD'
    GROUP BY "Dest"
),
t3 AS ( -- merge t1, t2 into one table
    SELECT t1.ap AS ap1, t1.cnt_in, t2.ap AS ap2, t2.cnt_out
    FROM t1 FULL OUTER JOIN t2 ON (t1.ap = t2.ap))
SELECT CASE WHEN ap1 IS NULL THEN ap2 ELSE ap1 END AS "Airport",
       cnt_in AS "InboundFlights", cnt_out AS "OutboundFlights"
FROM t3
ORDER BY "Airport"
;

