# Build all source-name mappings before replacing any raw table.
function source_name_queries(vars::Dict{String,String}, buckets::Int)
    p = vars["RAW_PREFIX"]
    header = """
    SET max_threads = $(vars["THREADS"]);
    SET max_insert_threads = 1;
    SET max_partitions_per_insert_block = $buckets;
    SET max_memory_usage = $(vars["MEMORY_BYTES"]);
    SET max_bytes_before_external_group_by = $(vars["SPILL_BYTES"]);
    SET max_bytes_before_external_sort = $(vars["SPILL_BYTES"]);
    SET join_algorithm = 'auto';
    SET join_use_nulls = 0;
    """
    queries = Pair{String,String}[]
    add(name, sql) = push!(queries, name => header * sql)
    maps = String[]
    function mapping(name, sql; partition="")
        push!(maps, name)
        add("map_$name", """
        DROP TABLE IF EXISTS $(p)name_$name;
        CREATE TABLE $(p)name_$name ENGINE=MergeTree $partition ORDER BY tuple() AS
        $sql;
        """)
    end
    mapping("routes", """
        SELECT DISTINCT r.source AS source, r.route_id AS route_id,
            if(s.source_file = '', r.source, s.source_file) AS new_source
        FROM $(p)routes r
        LEFT JOIN $(p)route_sources rs ON r.source = rs.source AND r.route_id = rs.route_id
        LEFT JOIN $(p)sources s ON rs.source = s.source AND rs.origin_source = s.origin_source
        """)
    mapping("trips", """
        SELECT t.source AS source, t.trip_id AS trip_id, t.service_id AS service_id,
            t.shape_id AS shape_id, n.new_source AS new_source
        FROM $(p)trips t
        INNER JOIN $(p)name_routes n ON t.source = n.source AND t.route_id = n.route_id
        """; partition="PARTITION BY sipHash64(source, trip_id) % $buckets")
    for (name, key) in (("services", "service_id"), ("shapes", "shape_id"))
        mapping(name, "SELECT DISTINCT source, $key, new_source FROM $(p)name_trips WHERE $key != ''")
    end
    mapping("agency", """
        SELECT DISTINCT r.source AS source, r.agency_id AS agency_id, n.new_source AS new_source
        FROM $(p)routes r
        INNER JOIN $(p)name_routes n ON r.source = n.source AND r.route_id = n.route_id
        """)
    mapping("feeds", "SELECT DISTINCT source, new_source FROM $(p)name_routes")
    mapping("stops", "SELECT source, stop_id, source AS new_source FROM $(p)stops WHERE 0")
    for bucket in 0:buckets-1
        filter = "sipHash64(source, trip_id) % $buckets = $bucket"
        add("map_stops_$bucket", """
            INSERT INTO $(p)name_stops
            SELECT DISTINCT st.source, st.stop_id, n.new_source
            FROM (SELECT source, trip_id, stop_id FROM $(p)stop_times WHERE $filter) st
            INNER JOIN (SELECT source, trip_id, new_source FROM $(p)name_trips WHERE $filter) n
                ON st.source = n.source AND st.trip_id = n.trip_id;
            """)
    end
    add("compact_stops", """
        DROP TABLE IF EXISTS $(p)name_stops_compact;
        CREATE TABLE $(p)name_stops_compact ENGINE=MergeTree ORDER BY (source, stop_id) AS
        SELECT DISTINCT source, stop_id, new_source FROM $(p)name_stops;
        DROP TABLE $(p)name_stops;
        RENAME TABLE $(p)name_stops_compact TO $(p)name_stops;
        """)
    # GTFS has at most two parent links: boarding area -> platform -> station.
    for level in 1:2
        add("parent_stops_$level", """
            INSERT INTO $(p)name_stops
            SELECT DISTINCT s.source, s.parent_station AS stop_id, n.new_source
            FROM $(p)stops s INNER JOIN $(p)name_stops n
                ON s.source = n.source AND s.stop_id = n.stop_id
            WHERE s.parent_station != '';
            """)
    end
    for level in 1:2
        add("child_stops_$level", """
            INSERT INTO $(p)name_stops
            SELECT DISTINCT s.source, s.stop_id, n.new_source
            FROM $(p)stops s INNER JOIN $(p)name_stops n
                ON s.source = n.source AND s.parent_station = n.stop_id;
            """)
    end
    # Dataset-wide credits apply to every attributed feed. Scoped credits keep
    # their route, trip, or agency scope. Credit names do not become sources.
    mapping("attributions", join([
        """
        SELECT DISTINCT a.source AS source, a.attribution_id AS attribution_id, n.new_source AS new_source
        FROM $(p)attributions a INNER JOIN $(p)name_$map n
            ON a.source = n.source $condition
        WHERE $scope
        """
        for (map, condition, scope) in (
            ("routes", "AND a.route_id = n.route_id", "a.route_id != ''"),
            ("trips", "AND a.trip_id = n.trip_id", "a.route_id = '' AND a.trip_id != ''"),
            ("agency", "AND a.agency_id = n.agency_id", "a.route_id = '' AND a.trip_id = '' AND a.agency_id != ''"),
            ("feeds", "", "a.route_id = '' AND a.trip_id = '' AND a.agency_id = ''"),
        )], "\nUNION DISTINCT\n"))
    mapping("sources", "SELECT DISTINCT source, origin_source, source_file AS new_source FROM $(p)sources")

    tables = [
        ("routes", "routes", "route_id"), ("trips", "trips", "trip_id"),
        ("stop_times", "trips", "trip_id"), ("shapes", "shapes", "shape_id"),
        ("calendar", "services", "service_id"), ("calendar_dates", "services", "service_id"),
        ("stops", "stops", "stop_id"), ("agency", "agency", "agency_id"),
        ("route_sources", "routes", "route_id"), ("sources", "sources", "origin_source"),
        ("attributions", "attributions", "attribution_id"),
    ]
    for (table, map, key) in tables
        add("create_$table", """
            DROP TABLE IF EXISTS $(p)named_$table;
            CREATE TABLE $(p)named_$table AS $(p)$table;
            """)
        source_file_match = table == "sources" ? " AND t.source_file = n.new_source" : ""
        for bucket in (key == "trip_id" ? (0:buckets-1) : (nothing,))
            filter = bucket === nothing ? "" : " WHERE sipHash64(source, trip_id) % $buckets = $bucket"
            add("$(table)_$(something(bucket, 0))", """
                INSERT INTO $(p)named_$table
                SELECT t.* REPLACE (toLowCardinality(if(n.new_source = '', t.source, n.new_source)) AS source)
                FROM (SELECT * FROM $(p)$table$filter) t
                LEFT JOIN (SELECT DISTINCT source, $key, new_source FROM $(p)name_$map$filter) n
                    ON t.source = n.source AND t.$key = n.$key$source_file_match
                WHERE n.new_source != '' OR t.source NOT IN
                    (SELECT source FROM $(p)name_feeds WHERE new_source != source);
                """)
        end
    end
    # Queries above read only original tables. Publish replacements after all
    # copies finish. A failed raw stage must be restarted, not resumed mid-copy.
    for (table, _, _) in tables
        add("publish_$table", """
            DROP TABLE IF EXISTS $(p)unnamed_$table;
            RENAME TABLE $(p)$table TO $(p)unnamed_$table, $(p)named_$table TO $(p)$table;
            DROP TABLE $(p)unnamed_$table;
            """)
    end
    add("cleanup", join(["DROP TABLE $(p)name_$map;" for map in maps], "\n"))
    return queries
end

function run_source_names(cfg, vars, temp_dir)
    for (name, sql) in source_name_queries(vars, cfg.trip_buckets)
        path = joinpath(temp_dir, "01_source_names_$name.sql")
        write(path, sql)
        run_sql_file(path, cfg.execute, cfg.timeout_seconds)
    end
end
