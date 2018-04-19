
def generate_schema(f, conf):
	print >>f, "-- DROP SCHEMA IF EXISTS atraf CASCADE;"
	print >>f, "CREATE SCHEMA atraf;"
	print >>f, "SET SCHEMA atraf;"
	print >>f, ""
	print >>f, "-- These tables remain empty but are used"
	print >>f, "-- as a template for CREATE TABLE LIKE."
	print >>f, ""
	print >>f, "CREATE TABLE \"ontime_template\" ("
	print >>f, "        \"Year\" smallint DEFAULT NULL,"
	print >>f, "        \"Quarter\" tinyint DEFAULT NULL,"
	print >>f, "        \"Month\" tinyint DEFAULT NULL,"
	print >>f, "        \"DayofMonth\" tinyint DEFAULT NULL,"
	print >>f, "        \"DayOfWeek\" tinyint DEFAULT NULL,"
	print >>f, "        \"FlightDate\" date DEFAULT NULL,"
	print >>f, "        \"UniqueCarrier\" char(7) DEFAULT NULL,"
	print >>f, "        \"AirlineID\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"Carrier\" char(2) DEFAULT NULL,"
	print >>f, "        \"TailNum\" varchar(50) DEFAULT NULL,"
	print >>f, "        \"FlightNum\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"OriginAirportID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"OriginAirportSeqID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"OriginCityMarketID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Origin\" char(5) DEFAULT NULL,"
	print >>f, "        \"OriginCityName\" varchar(100) DEFAULT NULL,"
	print >>f, "        \"OriginState\" char(2) DEFAULT NULL,"
	print >>f, "        \"OriginStateFips\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"OriginStateName\" varchar(100) DEFAULT NULL,"
	print >>f, "        \"OriginWac\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"DestAirportID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"DestAirportSeqID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"DestCityMarketID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Dest\" char(5) DEFAULT NULL,"
	print >>f, "        \"DestCityName\" varchar(100) DEFAULT NULL,"
	print >>f, "        \"DestState\" char(2) DEFAULT NULL,"
	print >>f, "        \"DestStateFips\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"DestStateName\" varchar(100) DEFAULT NULL,"
	print >>f, "        \"DestWac\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"CRSDepTime\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"DepTime\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"DepDelay\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"DepDelayMinutes\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"DepDel15\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"DepartureDelayGroups\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"DepTimeBlk\" varchar(20) DEFAULT NULL,"
	print >>f, "        \"TaxiOut\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"WheelsOff\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"WheelsOn\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"TaxiIn\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"CRSArrTime\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"ArrTime\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"ArrDelay\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"ArrDelayMinutes\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"ArrDel15\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"ArrivalDelayGroups\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"ArrTimeBlk\" varchar(20) DEFAULT NULL,"
	print >>f, "        \"Cancelled\" tinyint DEFAULT NULL,"
	print >>f, "        \"CancellationCode\" char(1) DEFAULT NULL,"
	print >>f, "        \"Diverted\" tinyint DEFAULT NULL,"
	print >>f, "        \"CRSElapsedTime\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"ActualElapsedTime\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"AirTime\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"Flights\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"Distance\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"DistanceGroup\" tinyint DEFAULT NULL,"
	print >>f, "        \"CarrierDelay\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"WeatherDelay\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"NASDelay\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"SecurityDelay\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"LateAircraftDelay\" decimal(8,2) DEFAULT NULL,"
	print >>f, "        \"FirstDepTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"TotalAddGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"LongestAddGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"DivAirportLandings\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"DivReachedDest\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"DivActualElapsedTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"DivArrDelay\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"DivDistance\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div1Airport\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div1AirportID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div1AirportSeqID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div1WheelsOn\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div1TotalGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div1LongestGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div1WheelsOff\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div1TailNum\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div2Airport\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div2AirportID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div2AirportSeqID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div2WheelsOn\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div2TotalGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div2LongestGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div2WheelsOff\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div2TailNum\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div3Airport\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div3AirportID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div3AirportSeqID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div3WheelsOn\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div3TotalGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div3LongestGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div3WheelsOff\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div3TailNum\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div4Airport\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div4AirportID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div4AirportSeqID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div4WheelsOn\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div4TotalGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div4LongestGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div4WheelsOff\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div4TailNum\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div5Airport\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div5AirportID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div5AirportSeqID\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div5WheelsOn\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div5TotalGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div5LongestGTime\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div5WheelsOff\" varchar(10) DEFAULT NULL,"
	print >>f, "        \"Div5TailNum\" varchar(10) DEFAULT NULL"
	print >>f, ");"
	print >>f, ""
	print >>f, "CREATE TABLE tmp_template ("
	print >>f, "        \"Hour\" TINYINT, \"PredictedArrDelay\" DECIMAL(8,2) DEFAULT 0.0"
	print >>f, ");"
	print >>f, ""
	print >>f, "-- Every host has a local copy of tmp and a REPLICA TABLE of all of them."
	print >>f, ""

	if not conf.distributed:
		tmp_name = "tmp"
		print >>f, "CREATE TABLE \"tmp\" ( LIKE tmp_template );"
	else:
		tmp_name = "tmp_%s" % conf.node
		print >>f, "CREATE REPLICA TABLE tmp ( LIKE tmp_template );"
		for node in conf.nodes:
			if node == conf.node:
				print >>f, "CREATE TABLE \"tmp_%(node)s\" ( LIKE tmp_template );" % conf
			else:
				print >>f, "CREATE REMOTE TABLE \"tmp_%s\" (LIKE tmp_template)" % node,
				print >>f, "ON '%s';" % conf.urls[node]
				print >>f, "ALTER TABLE tmp ADD TABLE \"tmp_%s\";" % node
		print >>f, ""

	print >>f, "INSERT INTO \"%s\" (\"Hour\")" % tmp_name
	print >>f, "VALUES"
	print >>f, "    (0), (1), (2), (3), (4), (5), "
	print >>f, "    (6), (7), (8), (9), (10), (11), "
	print >>f, "    (12), (13), (14), (15), (16), (17), "
	print >>f, "    (18), (19), (20), (21), (22), (23);"
	print >>f, ""
	print >>f, ""

	print >>f, "-- Every slave has some part of the ontime data."
	print >>f, "-- Every node has a MERGE TABLE of all of them."
	print >>f, ""

	if not conf.distributed:
		print >>f, "CREATE TABLE \"ontime\" ( LIKE \"ontime_template\" );"
	else:
		print >>f, "CREATE MERGE TABLE \"ontime\" ( LIKE \"ontime_template\" );"
		for node in conf.nodes:
			if not conf.partitions[node]:
				continue
			if node == conf.node:
				print >>f, "CREATE TABLE \"ontime_%s\" ( LIKE \"ontime_template\" );" % node
			else:
				print >>f, "CREATE REMOTE TABLE \"ontime_%s\" ( LIKE \"ontime_template\" ) " % node
				print >>f, "    ON '%s';" % conf.urls[node]
			print >>f, "ALTER TABLE \"ontime\" ADD TABLE \"ontime_%s\";" % node

	print >>f, ""

	print >>f, "CREATE FUNCTION histo (categories TINYINT)"
	print >>f, "RETURNS TABLE (low INT, high INT, CntArrDelay INT)"
	print >>f, "BEGIN"
	print >>f, "    DECLARE minAD INT, sz INT, grp INT;"
	print >>f, "    SET minAD = (SELECT min(\"ArrDelay\") FROM ontime); "
	print >>f, "    SET sz = (SELECT (max(\"ArrDelay\") - minAD)/categories FROM ontime);"
	print >>f, "    SET grp = 0;"
	print >>f, ""
	print >>f, "    DECLARE TABLE tmp1(low INT, \"CntArrDelay\" INT DEFAULT 0);"
	print >>f, "    WHILE (grp < categories) DO"
	print >>f, "        INSERT INTO tmp1(low) VALUES (minAD + sz * grp);"
	print >>f, "        SET grp = grp + 1;"
	print >>f, "    END WHILE;"
	print >>f, ""
	print >>f, "    -- devide the ArrDelay values into groups"
	print >>f, "    INSERT INTO tmp1"
	print >>f, "    SELECT low, COUNT(*) AS \"CntArrDelay\""
	print >>f, "    FROM (SELECT minAD + sz * CAST(FLOOR((\"ArrDelay\" - minAD) / sz) AS INT) AS low"
	print >>f, "          FROM ontime"
	print >>f, "          WHERE \"ArrDelay\" IS NOT NULL) AS t"
	print >>f, "    GROUP BY low;"
	print >>f, ""
	print >>f, "    RETURN"
	print >>f, "        SELECT low, low + sz AS high, SUM(\"CntArrDelay\") AS \"CntArrDelay\""
	print >>f, "        FROM tmp1"
	print >>f, "        GROUP BY low"
	print >>f, "        ORDER BY low;"
	print >>f, "END;"
	print >>f, ""

	print >>f, "-- Used to check if COPY INTO BEST EFFORT lost any rows"
	print >>f, "CREATE TABLE expected_rows ( \"Year\" INT, \"Month\" INT, \"Rows\" INT );"
	all_parts = [part for _host, parts in conf.partitions.items() for part in parts]
	all_parts = sorted(all_parts, key=lambda p: (p.year, p.month))
	if all_parts:
		print >>f, "INSERT INTO expected_rows(\"Year\", \"Month\", \"Rows\") VALUES"
		for part in all_parts:
			end = "," if part != all_parts[-1] else ";"
			print >>f, "        (%d, %d, %d)%s" % (part.year, part.month, part.lines - 1, end)
	print >>f, ""

	print >>f, "CREATE VIEW missing_rows AS"
	print >>f, "SELECT"
	print >>f, "        e.\"Year\" AS \"Year\","
	print >>f, "        e.\"Month\" AS \"Month\","
	print >>f, "        e.\"Rows\" AS expected,"
	print >>f, "        (SELECT COUNT(*) FROM \"ontime\" AS o WHERE o.\"Year\" = e.\"Year\" AND o.\"Month\" = e.\"Month\") AS actual"
	print >>f, "FROM"
	print >>f, "        expected_rows AS e"
	print >>f, ";"

def generate_inserts(f, conf):
	print >>f, "SET SCHEMA atraf;"
	print >>f, ""

	if conf.distributed:
		table_name = "ontime_%s" % conf.node
		table_exists = len(conf.partition) > 0
	else:
		table_name = "ontime"
		table_exists = True

	if not table_exists:
		return

	print >>f, "DELETE FROM \"%s\";" % table_name
	print >>f
	
	for fragment in conf.partition:
		print >>f, "COPY %d OFFSET 2 RECORDS INTO \"%s\"" % (fragment.lines - 1, table_name)
		print >>f, "FROM '@DATA_DIR@/%s'" % fragment.file
		print >>f, "USING DELIMITERS ',','\\n','\"';"
		print >>f

	print >>f, "ALTER TABLE \"%s\" SET READ ONLY;" % table_name
	print >>f, "ANALYZE atraf.\"%s\";" % table_name
