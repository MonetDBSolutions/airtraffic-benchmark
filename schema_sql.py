
ATRAF_COLUMNS = [
        ("Year",                 "smallint DEFAULT NULL",                               "001_Year.sht.col"),
        ("Quarter",              "tinyint DEFAULT NULL",                             "002_Quarter.bte.col"),
        ("Month",                "tinyint DEFAULT NULL",                               "003_Month.bte.col"),
        ("DayofMonth",           "tinyint DEFAULT NULL",                          "004_DayofMonth.bte.col"),
        ("DayOfWeek",            "tinyint DEFAULT NULL",                           "005_DayOfWeek.bte.col"),
        ("FlightDate",           "date DEFAULT NULL",                             "006_FlightDate.date.col"),
        ("UniqueCarrier",        "char(7) DEFAULT NULL",                       "007_UniqueCarrier.str.col"),
        ("AirlineID",            "decimal(8,2) DEFAULT NULL",                      "008_AirlineID.int.col"),
        ("Carrier",              "char(2) DEFAULT NULL",                             "009_Carrier.str.col"),
        ("TailNum",              "varchar(50) DEFAULT NULL",                         "010_TailNum.str.col"),
        ("FlightNum",            "varchar(10) DEFAULT NULL",                       "011_FlightNum.str.col"),
        ("OriginAirportID",      "varchar(10) DEFAULT NULL",                 "012_OriginAirportID.str.col"),
        ("OriginAirportSeqID",   "varchar(10) DEFAULT NULL",              "013_OriginAirportSeqID.str.col"),
        ("OriginCityMarketID",   "varchar(10) DEFAULT NULL",              "014_OriginCityMarketID.str.col"),
        ("Origin",               "char(5) DEFAULT NULL",                              "015_Origin.str.col"),
        ("OriginCityName",       "varchar(100) DEFAULT NULL",                 "016_OriginCityName.str.col"),
        ("OriginState",          "char(2) DEFAULT NULL",                         "017_OriginState.str.col"),
        ("OriginStateFips",      "varchar(10) DEFAULT NULL",                 "018_OriginStateFips.str.col"),
        ("OriginStateName",      "varchar(100) DEFAULT NULL",                "019_OriginStateName.str.col"),
        ("OriginWac",            "decimal(8,2) DEFAULT NULL",                      "020_OriginWac.int.col"),
        ("DestAirportID",        "varchar(10) DEFAULT NULL",                   "021_DestAirportID.str.col"),
        ("DestAirportSeqID",     "varchar(10) DEFAULT NULL",                "022_DestAirportSeqID.str.col"),
        ("DestCityMarketID",     "varchar(10) DEFAULT NULL",                "023_DestCityMarketID.str.col"),
        ("Dest",                 "char(5) DEFAULT NULL",                                "024_Dest.str.col"),
        ("DestCityName",         "varchar(100) DEFAULT NULL",                   "025_DestCityName.str.col"),
        ("DestState",            "char(2) DEFAULT NULL",                           "026_DestState.str.col"),
        ("DestStateFips",        "varchar(10) DEFAULT NULL",                   "027_DestStateFips.str.col"),
        ("DestStateName",        "varchar(100) DEFAULT NULL",                  "028_DestStateName.str.col"),
        ("DestWac",              "decimal(8,2) DEFAULT NULL",                        "029_DestWac.int.col"),
        ("CRSDepTime",           "decimal(8,2) DEFAULT NULL",                     "030_CRSDepTime.int.col"),
        ("DepTime",              "decimal(8,2) DEFAULT NULL",                        "031_DepTime.int.col"),
        ("DepDelay",             "decimal(8,2) DEFAULT NULL",                       "032_DepDelay.int.col"),
        ("DepDelayMinutes",      "decimal(8,2) DEFAULT NULL",                "033_DepDelayMinutes.int.col"),
        ("DepDel15",             "decimal(8,2) DEFAULT NULL",                       "034_DepDel15.int.col"),
        ("DepartureDelayGroups", "decimal(8,2) DEFAULT NULL",           "035_DepartureDelayGroups.int.col"),
        ("DepTimeBlk",           "varchar(20) DEFAULT NULL",                      "036_DepTimeBlk.str.col"),
        ("TaxiOut",              "decimal(8,2) DEFAULT NULL",                        "037_TaxiOut.int.col"),
        ("WheelsOff",            "decimal(8,2) DEFAULT NULL",                      "038_WheelsOff.int.col"),
        ("WheelsOn",             "decimal(8,2) DEFAULT NULL",                       "039_WheelsOn.int.col"),
        ("TaxiIn",               "decimal(8,2) DEFAULT NULL",                         "040_TaxiIn.int.col"),
        ("CRSArrTime",           "decimal(8,2) DEFAULT NULL",                     "041_CRSArrTime.int.col"),
        ("ArrTime",              "decimal(8,2) DEFAULT NULL",                        "042_ArrTime.int.col"),
        ("ArrDelay",             "decimal(8,2) DEFAULT NULL",                       "043_ArrDelay.int.col"),
        ("ArrDelayMinutes",      "decimal(8,2) DEFAULT NULL",                "044_ArrDelayMinutes.int.col"),
        ("ArrDel15",             "decimal(8,2) DEFAULT NULL",                       "045_ArrDel15.int.col"),
        ("ArrivalDelayGroups",   "decimal(8,2) DEFAULT NULL",             "046_ArrivalDelayGroups.int.col"),
        ("ArrTimeBlk",           "varchar(20) DEFAULT NULL",                      "047_ArrTimeBlk.str.col"),
        ("Cancelled",            "tinyint DEFAULT NULL",                           "048_Cancelled.bte.col"),
        ("CancellationCode",     "char(1) DEFAULT NULL",                    "049_CancellationCode.str.col"),
        ("Diverted",             "tinyint DEFAULT NULL",                            "050_Diverted.bte.col"),
        ("CRSElapsedTime",       "decimal(8,2) DEFAULT NULL",                 "051_CRSElapsedTime.int.col"),
        ("ActualElapsedTime",    "decimal(8,2) DEFAULT NULL",              "052_ActualElapsedTime.int.col"),
        ("AirTime",              "decimal(8,2) DEFAULT NULL",                        "053_AirTime.int.col"),
        ("Flights",              "decimal(8,2) DEFAULT NULL",                        "054_Flights.int.col"),
        ("Distance",             "decimal(8,2) DEFAULT NULL",                       "055_Distance.int.col"),
        ("DistanceGroup",        "tinyint DEFAULT NULL",                       "056_DistanceGroup.bte.col"),
        ("CarrierDelay",         "decimal(8,2) DEFAULT NULL",                   "057_CarrierDelay.int.col"),
        ("WeatherDelay",         "decimal(8,2) DEFAULT NULL",                   "058_WeatherDelay.int.col"),
        ("NASDelay",             "decimal(8,2) DEFAULT NULL",                       "059_NASDelay.int.col"),
        ("SecurityDelay",        "decimal(8,2) DEFAULT NULL",                  "060_SecurityDelay.int.col"),
        ("LateAircraftDelay",    "decimal(8,2) DEFAULT NULL",              "061_LateAircraftDelay.int.col"),
        ("FirstDepTime",         "varchar(10) DEFAULT NULL",                    "062_FirstDepTime.str.col"),
        ("TotalAddGTime",        "varchar(10) DEFAULT NULL",                   "063_TotalAddGTime.str.col"),
        ("LongestAddGTime",      "varchar(10) DEFAULT NULL",                 "064_LongestAddGTime.str.col"),
        ("DivAirportLandings",   "varchar(10) DEFAULT NULL",              "065_DivAirportLandings.str.col"),
        ("DivReachedDest",       "varchar(10) DEFAULT NULL",                  "066_DivReachedDest.str.col"),
        ("DivActualElapsedTime", "varchar(10) DEFAULT NULL",            "067_DivActualElapsedTime.str.col"),
        ("DivArrDelay",          "varchar(10) DEFAULT NULL",                     "068_DivArrDelay.str.col"),
        ("DivDistance",          "varchar(10) DEFAULT NULL",                     "069_DivDistance.str.col"),
        ("Div1Airport",          "varchar(10) DEFAULT NULL",                     "070_Div1Airport.str.col"),
        ("Div1AirportID",        "varchar(10) DEFAULT NULL",                   "071_Div1AirportID.str.col"),
        ("Div1AirportSeqID",     "varchar(10) DEFAULT NULL",                "072_Div1AirportSeqID.str.col"),
        ("Div1WheelsOn",         "varchar(10) DEFAULT NULL",                    "073_Div1WheelsOn.str.col"),
        ("Div1TotalGTime",       "varchar(10) DEFAULT NULL",                  "074_Div1TotalGTime.str.col"),
        ("Div1LongestGTime",     "varchar(10) DEFAULT NULL",                "075_Div1LongestGTime.str.col"),
        ("Div1WheelsOff",        "varchar(10) DEFAULT NULL",                   "076_Div1WheelsOff.str.col"),
        ("Div1TailNum",          "varchar(10) DEFAULT NULL",                     "077_Div1TailNum.str.col"),
        ("Div2Airport",          "varchar(10) DEFAULT NULL",                     "078_Div2Airport.str.col"),
        ("Div2AirportID",        "varchar(10) DEFAULT NULL",                   "079_Div2AirportID.str.col"),
        ("Div2AirportSeqID",     "varchar(10) DEFAULT NULL",                "080_Div2AirportSeqID.str.col"),
        ("Div2WheelsOn",         "varchar(10) DEFAULT NULL",                    "081_Div2WheelsOn.str.col"),
        ("Div2TotalGTime",       "varchar(10) DEFAULT NULL",                  "082_Div2TotalGTime.str.col"),
        ("Div2LongestGTime",     "varchar(10) DEFAULT NULL",                "083_Div2LongestGTime.str.col"),
        ("Div2WheelsOff",        "varchar(10) DEFAULT NULL",                   "084_Div2WheelsOff.str.col"),
        ("Div2TailNum",          "varchar(10) DEFAULT NULL",                     "085_Div2TailNum.str.col"),
        ("Div3Airport",          "varchar(10) DEFAULT NULL",                     "086_Div3Airport.str.col"),
        ("Div3AirportID",        "varchar(10) DEFAULT NULL",                   "087_Div3AirportID.str.col"),
        ("Div3AirportSeqID",     "varchar(10) DEFAULT NULL",                "088_Div3AirportSeqID.str.col"),
        ("Div3WheelsOn",         "varchar(10) DEFAULT NULL",                    "089_Div3WheelsOn.str.col"),
        ("Div3TotalGTime",       "varchar(10) DEFAULT NULL",                  "090_Div3TotalGTime.str.col"),
        ("Div3LongestGTime",     "varchar(10) DEFAULT NULL",                "091_Div3LongestGTime.str.col"),
        ("Div3WheelsOff",        "varchar(10) DEFAULT NULL",                   "092_Div3WheelsOff.str.col"),
        ("Div3TailNum",          "varchar(10) DEFAULT NULL",                     "093_Div3TailNum.str.col"),
        ("Div4Airport",          "varchar(10) DEFAULT NULL",                     "094_Div4Airport.str.col"),
        ("Div4AirportID",        "varchar(10) DEFAULT NULL",                   "095_Div4AirportID.str.col"),
        ("Div4AirportSeqID",     "varchar(10) DEFAULT NULL",                "096_Div4AirportSeqID.str.col"),
        ("Div4WheelsOn",         "varchar(10) DEFAULT NULL",                    "097_Div4WheelsOn.str.col"),
        ("Div4TotalGTime",       "varchar(10) DEFAULT NULL",                  "098_Div4TotalGTime.str.col"),
        ("Div4LongestGTime",     "varchar(10) DEFAULT NULL",                "099_Div4LongestGTime.str.col"),
        ("Div4WheelsOff",        "varchar(10) DEFAULT NULL",                   "100_Div4WheelsOff.str.col"),
        ("Div4TailNum",          "varchar(10) DEFAULT NULL",                     "101_Div4TailNum.str.col"),
        ("Div5Airport",          "varchar(10) DEFAULT NULL",                     "102_Div5Airport.str.col"),
        ("Div5AirportID",        "varchar(10) DEFAULT NULL",                   "103_Div5AirportID.str.col"),
        ("Div5AirportSeqID",     "varchar(10) DEFAULT NULL",                "104_Div5AirportSeqID.str.col"),
        ("Div5WheelsOn",         "varchar(10) DEFAULT NULL",                    "105_Div5WheelsOn.str.col"),
        ("Div5TotalGTime",       "varchar(10) DEFAULT NULL",                  "106_Div5TotalGTime.str.col"),
        ("Div5LongestGTime",     "varchar(10) DEFAULT NULL",                "107_Div5LongestGTime.str.col"),
        ("Div5WheelsOff",        "varchar(10) DEFAULT NULL",                   "108_Div5WheelsOff.str.col"),
        ("Div5TailNum",          "varchar(10) DEFAULT NULL",                     "109_Div5TailNum.str.col"),
]




BEFORE1 = """
-- DROP SCHEMA IF EXISTS atraf CASCADE;
CREATE SCHEMA atraf;
SET SCHEMA atraf;

-- These tables are used as a template for other tables,
-- which are created using CREATE TABLE t (LIKE t_template).

CREATE TABLE "ontime_template" (
"""

BEFORE2 = """
);

CREATE TABLE tmp_template (
        "Hour" TINYINT, "PredictedArrDelay" DECIMAL(8,2) DEFAULT 0.0
);
"""

BEFORE = (
	BEFORE1
	+ ",\n".join([f'        "{c}" {d}' for (c, d, f) in ATRAF_COLUMNS])
	+ BEFORE2
)

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
		if conf.binary:
			ext = '.' + conf.compression if conf.load_compressed else ''
			rfiles = [f"        R'@DOWNLOAD_DIR@/binary/{fragment.name}/{f}{ext}'" for (c, d, f) in ATRAF_COLUMNS]
			print(f"COPY BINARY INTO \"{table_name}\"", file=f)
			print(f"FROM", file=f)
			print(",\n".join(rfiles), file=f)
			#print("    ON CLIENT", file=f)
			print(";", file=f)
			print(file=f)
		else:
			print(f"COPY {fragment.lines - 1} OFFSET 2 RECORDS INTO \"{table_name}\"", file=f)
			print(f"FROM '@DOWNLOAD_DIR@/{fragment.load_file}'", file=f)
			#print("    ON CLIENT", file=f)
			print("USING DELIMITERS ',','\\n','\"';", file=f)
			print(file=f)

	print("COMMIT;", file=f)
	print(file=f)

	#print(f"ALTER TABLE \"{table_name}\" SET READ ONLY;", file=f)
	#print(f"ANALYZE atraf.\"{table_name}\";", file=f)

