using Dates
using ClickHouse

include("stop_uuid_generator.jl") 
include("lib.jl")

# can provide own yyyymmdd to ... lie
target_date = length(ARGS) > 0 ? Date(ARGS[1], "yyyymmdd") : today()
date_str = Dates.format(target_date, "yyyymmdd")
date_dir_name = Dates.format(target_date, "yyyy-mm-dd")

project_root = dirname(@__DIR__)
data_dir = joinpath(project_root, "data", date_dir_name)
grabber_script = joinpath(@__DIR__, "grabber.sh")
ingest_sql_path_part_1 = joinpath(@__DIR__, "part_1.sql")
ingest_sql_path_part_2 = joinpath(@__DIR__, "part_2.sql")

if !isdir(data_dir)
    println(">>> Creating directory and downloading files...")
    mkdir(data_dir)
    cmd = `bash -c "cd $data_dir && $grabber_script $data_dir"`
    try 
        run(cmd)
    catch e
        println(">>> Grabber completed (or warned).")
    end
else
    println(">>> Directory exists. Skipping download (remove folder to force re-download).")
end

println(">>> Processing SQL...")
sql_content_part_1 = read(ingest_sql_path_part_1, String)
sql_content_part_2 = read(ingest_sql_path_part_2, String)

fixed_path = "chungus/transitous/$date_dir_name/source=*"
table_prefix_old = "transitous_everything_"
table_prefix_new = "transitous_everything_$(date_str)_"
sql_content_part_1 = replace(sql_content_part_1, "transitous/source=*" => fixed_path)
sql_content_part_1 = replace(sql_content_part_1, table_prefix_old => table_prefix_new)
sql_content_part_2 = replace(sql_content_part_2, "transitous/source=*" => fixed_path)
sql_content_part_2 = replace(sql_content_part_2, table_prefix_old => table_prefix_new)

# make sure you have the env vars / ssh tunnel up
function run_sql_via_client(sql_content, part_name)
    temp_dir = joinpath(@__DIR__, "temp")
    if !isdir(temp_dir)
        mkdir(temp_dir)
    end
    
    temp_file = joinpath(temp_dir, "$part_name.sql")
    write(temp_file, sql_content)
    # cmd = `clickhouse-client --host $CLICKHOUSE_HOST --port $CLICKHOUSE_NATIVE_PORT --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD --database $CLICKHOUSE_DB --queries-file $temp_file`
    # run(cmd)
end

run_sql_via_client(sql_content_part_1, "part_1")

stops_table = "$(table_prefix_new)stops"
uuids_table = "$(table_prefix_new)stop_uuids"

conn = con()
generate_uuids(conn, stops_table, uuids_table)

run_sql_via_client(sql_content_part_2, "part_2")

println(">>> Orchestration complete!")
