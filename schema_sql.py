
BEFORE = """
-- DROP SCHEMA IF EXISTS atraf CASCADE;
CREATE SCHEMA atraf;
SET SCHEMA atraf;

-- These tables are used as a template for other tables,
-- which are created using CREATE TABLE t (LIKE t_template).

CREATE TABLE "ontime_template" (
        "Year" smallint DEFAULT NULL,
        "Quarter" tinyint DEFAULT NULL,
        "Month" tinyint DEFAULT NULL,
        "DayofMonth" tinyint DEFAULT NULL,
        "DayOfWeek" tinyint DEFAULT NULL,
        "FlightDate" date DEFAULT NULL,
        "UniqueCarrier" char(7) DEFAULT NULL,
        "AirlineID" decimal(8,2) DEFAULT NULL,
        "Carrier" char(2) DEFAULT NULL,
        "TailNum" varchar(50) DEFAULT NULL,
        "FlightNum" varchar(10) DEFAULT NULL,
        "OriginAirportID" varchar(10) DEFAULT NULL,
        "OriginAirportSeqID" varchar(10) DEFAULT NULL,
        "OriginCityMarketID" varchar(10) DEFAULT NULL,
        "Origin" char(5) DEFAULT NULL,
        "OriginCityName" varchar(100) DEFAULT NULL,
        "OriginState" char(2) DEFAULT NULL,
        "OriginStateFips" varchar(10) DEFAULT NULL,
        "OriginStateName" varchar(100) DEFAULT NULL,
        "OriginWac" decimal(8,2) DEFAULT NULL,
        "DestAirportID" varchar(10) DEFAULT NULL,
        "DestAirportSeqID" varchar(10) DEFAULT NULL,
        "DestCityMarketID" varchar(10) DEFAULT NULL,
        "Dest" char(5) DEFAULT NULL,
        "DestCityName" varchar(100) DEFAULT NULL,
        "DestState" char(2) DEFAULT NULL,
        "DestStateFips" varchar(10) DEFAULT NULL,
        "DestStateName" varchar(100) DEFAULT NULL,
        "DestWac" decimal(8,2) DEFAULT NULL,
        "CRSDepTime" decimal(8,2) DEFAULT NULL,
        "DepTime" decimal(8,2) DEFAULT NULL,
        "DepDelay" decimal(8,2) DEFAULT NULL,
        "DepDelayMinutes" decimal(8,2) DEFAULT NULL,
        "DepDel15" decimal(8,2) DEFAULT NULL,
        "DepartureDelayGroups" decimal(8,2) DEFAULT NULL,
        "DepTimeBlk" varchar(20) DEFAULT NULL,
        "TaxiOut" decimal(8,2) DEFAULT NULL,
        "WheelsOff" decimal(8,2) DEFAULT NULL,
        "WheelsOn" decimal(8,2) DEFAULT NULL,
        "TaxiIn" decimal(8,2) DEFAULT NULL,
        "CRSArrTime" decimal(8,2) DEFAULT NULL,
        "ArrTime" decimal(8,2) DEFAULT NULL,
        "ArrDelay" decimal(8,2) DEFAULT NULL,
        "ArrDelayMinutes" decimal(8,2) DEFAULT NULL,
        "ArrDel15" decimal(8,2) DEFAULT NULL,
        "ArrivalDelayGroups" decimal(8,2) DEFAULT NULL,
        "ArrTimeBlk" varchar(20) DEFAULT NULL,
        "Cancelled" tinyint DEFAULT NULL,
        "CancellationCode" char(1) DEFAULT NULL,
        "Diverted" tinyint DEFAULT NULL,
        "CRSElapsedTime" decimal(8,2) DEFAULT NULL,
        "ActualElapsedTime" decimal(8,2) DEFAULT NULL,
        "AirTime" decimal(8,2) DEFAULT NULL,
        "Flights" decimal(8,2) DEFAULT NULL,
        "Distance" decimal(8,2) DEFAULT NULL,
        "DistanceGroup" tinyint DEFAULT NULL,
        "CarrierDelay" decimal(8,2) DEFAULT NULL,
        "WeatherDelay" decimal(8,2) DEFAULT NULL,
        "NASDelay" decimal(8,2) DEFAULT NULL,
        "SecurityDelay" decimal(8,2) DEFAULT NULL,
        "LateAircraftDelay" decimal(8,2) DEFAULT NULL,
        "FirstDepTime" varchar(10) DEFAULT NULL,
        "TotalAddGTime" varchar(10) DEFAULT NULL,
        "LongestAddGTime" varchar(10) DEFAULT NULL,
        "DivAirportLandings" varchar(10) DEFAULT NULL,
        "DivReachedDest" varchar(10) DEFAULT NULL,
        "DivActualElapsedTime" varchar(10) DEFAULT NULL,
        "DivArrDelay" varchar(10) DEFAULT NULL,
        "DivDistance" varchar(10) DEFAULT NULL,
        "Div1Airport" varchar(10) DEFAULT NULL,
        "Div1AirportID" varchar(10) DEFAULT NULL,
        "Div1AirportSeqID" varchar(10) DEFAULT NULL,
        "Div1WheelsOn" varchar(10) DEFAULT NULL,
        "Div1TotalGTime" varchar(10) DEFAULT NULL,
        "Div1LongestGTime" varchar(10) DEFAULT NULL,
        "Div1WheelsOff" varchar(10) DEFAULT NULL,
        "Div1TailNum" varchar(10) DEFAULT NULL,
        "Div2Airport" varchar(10) DEFAULT NULL,
        "Div2AirportID" varchar(10) DEFAULT NULL,
        "Div2AirportSeqID" varchar(10) DEFAULT NULL,
        "Div2WheelsOn" varchar(10) DEFAULT NULL,
        "Div2TotalGTime" varchar(10) DEFAULT NULL,
        "Div2LongestGTime" varchar(10) DEFAULT NULL,
        "Div2WheelsOff" varchar(10) DEFAULT NULL,
        "Div2TailNum" varchar(10) DEFAULT NULL,
        "Div3Airport" varchar(10) DEFAULT NULL,
        "Div3AirportID" varchar(10) DEFAULT NULL,
        "Div3AirportSeqID" varchar(10) DEFAULT NULL,
        "Div3WheelsOn" varchar(10) DEFAULT NULL,
        "Div3TotalGTime" varchar(10) DEFAULT NULL,
        "Div3LongestGTime" varchar(10) DEFAULT NULL,
        "Div3WheelsOff" varchar(10) DEFAULT NULL,
        "Div3TailNum" varchar(10) DEFAULT NULL,
        "Div4Airport" varchar(10) DEFAULT NULL,
        "Div4AirportID" varchar(10) DEFAULT NULL,
        "Div4AirportSeqID" varchar(10) DEFAULT NULL,
        "Div4WheelsOn" varchar(10) DEFAULT NULL,
        "Div4TotalGTime" varchar(10) DEFAULT NULL,
        "Div4LongestGTime" varchar(10) DEFAULT NULL,
        "Div4WheelsOff" varchar(10) DEFAULT NULL,
        "Div4TailNum" varchar(10) DEFAULT NULL,
        "Div5Airport" varchar(10) DEFAULT NULL,
        "Div5AirportID" varchar(10) DEFAULT NULL,
        "Div5AirportSeqID" varchar(10) DEFAULT NULL,
        "Div5WheelsOn" varchar(10) DEFAULT NULL,
        "Div5TotalGTime" varchar(10) DEFAULT NULL,
        "Div5LongestGTime" varchar(10) DEFAULT NULL,
        "Div5WheelsOff" varchar(10) DEFAULT NULL,
        "Div5TailNum" varchar(10) DEFAULT NULL
);

CREATE TABLE tmp_template (
        "Hour" TINYINT, "PredictedArrDelay" DECIMAL(8,2) DEFAULT 0.0
);
"""

AFTER = """
CREATE FUNCTION histo (categories TINYINT)
RETURNS TABLE (low INT, high INT, CntArrDelay INT)
BEGIN
    DECLARE minAD INT, sz INT, grp INT;
    SET minAD = (SELECT min("ArrDelay") FROM ontime);
    SET sz = (SELECT (max("ArrDelay") - minAD)/categories FROM ontime);
    SET grp = 0;

    DECLARE TABLE tmp1(low INT, "CntArrDelay" INT DEFAULT 0);
    WHILE (grp < categories) DO
        INSERT INTO tmp1(low) VALUES (minAD + sz * grp);
        SET grp = grp + 1;
    END WHILE;

    -- devide the ArrDelay values into groups
    INSERT INTO tmp1
    SELECT low, COUNT(*) AS "CntArrDelay"
    FROM (SELECT minAD + sz * CAST(FLOOR(("ArrDelay" - minAD) / sz) AS INT) AS low
          FROM ontime
          WHERE "ArrDelay" IS NOT NULL) AS t
    GROUP BY low;

    RETURN
        SELECT low, low + sz AS high, SUM("CntArrDelay") AS "CntArrDelay"
        FROM tmp1
        GROUP BY low
        ORDER BY low;
END;

CREATE VIEW missing_rows AS
WITH e AS (
	SELECT "Year", "Month", SUM("Rows") AS expected
	FROM expected_rows
	GROUP BY "Year", "Month"
),
a AS (
	SELECT "Year", "Month", COUNT(*)  AS actual
	FROM ontime
	GROUP BY "Year", "Month"
)
SELECT e."Year", e."Month", expected, actual
FROM e FULL OUTER JOIN a ON e."Year" = a."Year" AND e."Month" = a."Month"
WHERE expected IS NULL OR actual IS NULL OR expected <> actual
ORDER BY "Year", "Month"
;
"""

def generate_local_schema(f, conf):
	global BEFORE, AFTER
	print(BEFORE, file=f)

	if conf.distributed:
		tmp_data = "tmp_node%d" % conf.nodenum
		print("CREATE REPLICA TABLE tmp (LIKE tmp_template);", file=f)
		print(f"CREATE TABLE \"{tmp_data}\" (LIKE tmp_template);", file=f)
		print(f"ALTER TABLE tmp ADD TABLE \"{tmp_data}\";", file=f)
		print("-- The other parts of the replica table will be added later", file=f)
	else:
		tmp_data = "tmp"
		print("CREATE TABLE tmp (LIKE tmp_template);", file=f)
	print("", file=f)

	print(f"INSERT INTO \"{tmp_data}\" (\"Hour\")", file=f)
	print("VALUES", file=f)
	print("    (0), (1), (2), (3), (4), (5), ", file=f)
	print("    (6), (7), (8), (9), (10), (11), ", file=f)
	print("    (12), (13), (14), (15), (16), (17), ", file=f)
	print("    (18), (19), (20), (21), (22), (23);", file=f)
	print("", file=f)

	if conf.distributed:
		print("CREATE MERGE TABLE ontime (LIKE ontime_template);", file=f)
		print("CREATE TABLE \"ontime_node%d\" (LIKE ontime_template);" % conf.nodenum, file=f)
		print("ALTER TABLE ontime ADD TABLE \"ontime_node%d\";" % conf.nodenum, file=f)
	else:
		print("CREATE TABLE ontime (LIKE ontime_template);", file=f)
	print("", file=f)

	print("-- Used to check if COPY INTO BEST EFFORT lost any rows", file=f)
	print("CREATE TABLE expected_rows ( \"Year\" INT, \"Month\" INT, \"Rows\" INT );", file=f)
	all_parts = [part for _host, parts in list(conf.partitions.items()) for part in parts]
	all_parts = sorted(all_parts, key=lambda p: (p.year, p.month))
	if all_parts:
		print("INSERT INTO expected_rows(\"Year\", \"Month\", \"Rows\") VALUES", file=f)
		for part in all_parts:
			end = "," if part != all_parts[-1] else ";"
			print("        (%d, %d, %d)%s" % (part.year, part.month, part.lines - 1, end), file=f)
	print("", file=f)

	print(AFTER, file=f)


def generate_remote_schema(f, conf):
	print("SET SCHEMA atraf;", file=f)
	print("", file=f)

	if not conf.distributed:
		return

	for i, node in enumerate(conf.nodes):
		if node != conf.node:
			print("CREATE REMOTE TABLE \"tmp_node%d\" (LIKE tmp_template)" % i, end=' ', file=f)
			print(f"ON '{conf.urls[node]}';", file=f)
			print("ALTER TABLE tmp ADD TABLE \"tmp_node%d\";" % i, file=f)
		print("", file=f)

	for i, node in enumerate(conf.nodes):
		if not conf.partitions[node]:
			continue
		if node != conf.node:
			print("CREATE REMOTE TABLE \"ontime_node%d\" ( LIKE \"ontime_template\" ) " % i, file=f)
			print(f"    ON '{conf.urls[node]}';", file=f)
			print("ALTER TABLE \"ontime\" ADD TABLE \"ontime_node%d\";" % i, file=f)
	print("", file=f)



def generate_inserts(f, conf):
	print("SET SCHEMA atraf;", file=f)
	print("", file=f)

	if conf.distributed:
		table_name = "ontime_node%d" % conf.nodenum
		table_exists = len(conf.partition) > 0
	else:
		table_name = "ontime"
		table_exists = True

	if not table_exists:
		return

	print(f"DELETE FROM \"{table_name}\";", file=f)
	print(file=f)

	print("START TRANSACTION;", file=f)
	print(file=f)

	for fragment in conf.partition:
		print(f"COPY {fragment.lines - 1} OFFSET 2 RECORDS INTO \"{table_name}\"", file=f)
		print(f"FROM '@DOWNLOAD_DIR@/{fragment.load_file}'", file=f)
		print("USING DELIMITERS ',','\\n','\"';", file=f)
		print(file=f)

	print("COMMIT;", file=f)
	print(file=f)

	print(f"ALTER TABLE \"{table_name}\" SET READ ONLY;", file=f)
	print(f"ANALYZE atraf.\"{table_name}\";", file=f)

