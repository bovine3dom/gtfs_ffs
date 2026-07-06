#!/usr/bin/env julia

using Dates

const PROJECT_ROOT = dirname(@__DIR__)
const SQL_DIR = joinpath(@__DIR__, "sql")

mutable struct Config
    target_date::Date
    data_root::String
    input_root::Union{Nothing,String}
    sources::Vector{Pair{String,String}}
    modes::Vector{String}
    fixture::Union{Nothing,String}
    execute::Bool
    download_transitous::Bool
    replace_sources::Bool
    min_date::Date
    max_date::Date
end

function usage()
    println("""
    Usage: julia --project=tidied_up tidied_up/orchestrate.jl [yyyymmdd] [options]

    Options:
      --date yyyymmdd             Ingestion/table date. Positional yyyymmdd still works.
      --data-root PATH            Root containing dated GTFS staging dirs. Default: PROJECT/data.
      --input-root PATH           Existing ClickHouse-visible directory containing source=*/.
      --source name=PATH          Copy a GTFS directory or .zip into DATA_ROOT/YYYY-MM-DD/source=name/.
                                  Relative paths are resolved under DATA_ROOT first. Can be repeated.
      --replace-sources           Replace staged source directories when --source/--fixture is used.
      --fixture fantasy           Generate a tiny local fixture that exercises the fantasy selector.
      --mode fantasy|real|both    Timetable outputs to build. Default: both.
                                  fantasy is intentionally silent: transitous_everything_yyyymmdd_*.
                                  real is explicit: transitous_everything_yyyymmdd_real_*.
      --min-date yyyy-mm-dd       Calendar search lower bound. Default: 2020-12-01.
      --max-date yyyy-mm-dd       Calendar search upper bound. Default: 2030-01-01.
      --download-transitous       Opt in to grabber.sh. Do not use for normal/toy runs.
      --execute                   Run clickhouse-client and stop UUID generation. Without this, only render SQL.
      --help                      Show this help.
    """)
end

function parse_date_arg(s::AbstractString)
    occursin(r"^\d{8}$", s) && return Date(s, "yyyymmdd")
    return Date(s)
end

function parse_args(args)
    cfg = Config(
        today(),
        joinpath(PROJECT_ROOT, "data"),
        nothing,
        Pair{String,String}[],
        ["fantasy", "real"],
        nothing,
        false,
        false,
        false,
        Date("2020-12-01"),
        Date("2030-01-01"),
    )

    date_was_set = false
    i = 1
    while i <= length(args)
        arg = args[i]

        take_value(flag) = begin
            i == length(args) && error("$flag needs a value")
            i += 1
            args[i]
        end

        if arg == "--help" || arg == "-h"
            usage()
            exit(0)
        elseif arg == "--date"
            cfg.target_date = parse_date_arg(take_value(arg))
            date_was_set = true
        elseif startswith(arg, "--date=")
            cfg.target_date = parse_date_arg(arg[8:end])
            date_was_set = true
        elseif arg == "--input-root"
            cfg.input_root = take_value(arg)
        elseif startswith(arg, "--input-root=")
            cfg.input_root = arg[14:end]
        elseif arg == "--data-root"
            cfg.data_root = abspath(take_value(arg))
        elseif startswith(arg, "--data-root=")
            cfg.data_root = abspath(arg[13:end])
        elseif arg == "--source"
            push!(cfg.sources, parse_source_spec(take_value(arg)))
        elseif startswith(arg, "--source=")
            push!(cfg.sources, parse_source_spec(arg[10:end]))
        elseif arg == "--fixture"
            cfg.fixture = take_value(arg)
        elseif startswith(arg, "--fixture=")
            cfg.fixture = arg[11:end]
        elseif arg == "--mode"
            cfg.modes = parse_modes(take_value(arg))
        elseif startswith(arg, "--mode=")
            cfg.modes = parse_modes(arg[8:end])
        elseif arg == "--min-date"
            cfg.min_date = Date(take_value(arg))
        elseif startswith(arg, "--min-date=")
            cfg.min_date = Date(arg[12:end])
        elseif arg == "--max-date"
            cfg.max_date = Date(take_value(arg))
        elseif startswith(arg, "--max-date=")
            cfg.max_date = Date(arg[12:end])
        elseif arg == "--execute"
            cfg.execute = true
        elseif arg == "--download-transitous"
            cfg.download_transitous = true
        elseif arg == "--replace-sources"
            cfg.replace_sources = true
        elseif startswith(arg, "--")
            error("Unknown option: $arg")
        elseif !date_was_set
            cfg.target_date = parse_date_arg(arg)
            date_was_set = true
        else
            error("Unexpected positional argument: $arg")
        end

        i += 1
    end

    return cfg
end

function parse_source_spec(spec::AbstractString)
    parts = split(spec, "="; limit=2)
    length(parts) == 2 || error("Source must be name=/path/to/feed_or_zip, got: $spec")
    name, source_path = parts
    occursin(r"^[A-Za-z0-9_.-]+$", name) || error("Source name '$name' is not safe for source=<name>; use letters, digits, _, -, or .")
    isempty(source_path) && error("Source path is empty for source '$name'")
    return String(name) => String(source_path)
end

function resolve_source_path(src::AbstractString, data_root::AbstractString)
    candidates = String[]
    if isabspath(src)
        push!(candidates, abspath(src))
    else
        push!(candidates, joinpath(data_root, src))

        parent = dirname(src)
        leaf = basename(src)
        if !startswith(leaf, "source=")
            staged_leaf = "source=$leaf"
            if isempty(parent) || parent == "."
                push!(candidates, joinpath(data_root, staged_leaf))
            else
                push!(candidates, joinpath(data_root, parent, staged_leaf))
            end
        end

        # Keep cwd-relative paths working as a fallback, but prefer DATA_ROOT-relative paths.
        push!(candidates, abspath(src))
    end

    for candidate in unique(candidates)
        ispath(candidate) && return candidate
    end

    error("Source path does not exist: $src. Tried:\n  $(join(unique(candidates), "\n  "))")
end

function parse_modes(raw::AbstractString)
    bits = lowercase.(strip.(split(raw, ",")))
    modes = String[]
    for bit in bits
        if bit == "both"
            append!(modes, ["fantasy", "real"])
        elseif bit in ("fantasy", "real")
            push!(modes, bit)
        else
            error("Unknown mode '$bit'; expected fantasy, real, or both")
        end
    end
    return unique(modes)
end

date_str(date::Date) = Dates.format(date, "yyyymmdd")
date_dir_name(date::Date) = Dates.format(date, "yyyy-mm-dd")

function maybe_source_glob(path::AbstractString)
    occursin("source=*", path) && return path
    return joinpath(path, "source=*")
end

function sql_escape(value::AbstractString)
    return replace(value, "'" => "''")
end

function render_template(template_name::AbstractString, vars::Dict{String,String}, out_path::AbstractString)
    template = read(joinpath(SQL_DIR, template_name), String)
    rendered = template
    for (key, value) in vars
        rendered = replace(rendered, "{{$(key)}}" => value)
    end
    leftovers = collect(eachmatch(r"\{\{[A-Z_]+\}\}", rendered))
    isempty(leftovers) || error("Unrendered placeholders in $template_name: $(join(unique(m.match for m in leftovers), ", "))")
    write(out_path, rendered)
    return out_path
end

function clickhouse_client_cmd(sql_file::AbstractString)
    clickhouse_user = get(ENV, "CLICKHOUSE_USER", "admin")
    clickhouse_db = get(ENV, "CLICKHOUSE_DB", "default")
    clickhouse_password = get(ENV, "CLICKHOUSE_PASSWORD", "")
    clickhouse_port = parse(Int, get(ENV, "CLICKHOUSE_NATIVE_PORT", "9000"))
    clickhouse_host = get(ENV, "CLICKHOUSE_HOST", "localhost")
    return `clickhouse-client --host=$clickhouse_host --port=$clickhouse_port --user=$clickhouse_user --password=$clickhouse_password --database=$clickhouse_db --receive_timeout=40000 --send_timeout=40000 --max_execution_time=0 --max_result_rows=0 --max_result_bytes=0 --queries-file=$sql_file`
end

function run_sql_file(sql_file::AbstractString, execute::Bool)
    if execute
        println(">>> Running $(basename(sql_file))")
        run(clickhouse_client_cmd(sql_file))
    else
        println(">>> Rendered $(sql_file)")
    end
end

function find_gtfs_root(path::AbstractString)
    required = Set(["routes.txt", "trips.txt", "stops.txt", "stop_times.txt"])
    if isdir(path)
        for (root, _, files) in walkdir(path)
            issubset(required, Set(files)) && return root
        end
    end
    error("Could not find a GTFS directory with routes.txt, trips.txt, stops.txt, and stop_times.txt under $path")
end

const GTFS_HEADERS = Dict(
    "agency.txt" => "agency_id,agency_name,agency_url,agency_timezone,agency_email,agency_fare_url,agency_lang,agency_phone\n",
    "routes.txt" => "route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color,route_sort_order,continuous_pickup,continuous_drop_off\n",
    "trips.txt" => "route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,bikes_allowed\n",
    "stops.txt" => "stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,stop_timezone,wheelchair_boarding,level_id,platform_code\n",
    "calendar.txt" => "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n",
    "calendar_dates.txt" => "service_id,date,exception_type\n",
    "stop_times.txt" => "trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,continuous_pickup,continuous_drop_off,shape_dist_traveled,timepoint,local_zone_id\n",
    "shapes.txt" => "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled\n",
)

function copy_gtfs_dir(src::AbstractString, target::AbstractString)
    root = find_gtfs_root(src)
    mkpath(target)
    for file in readdir(root)
        src_file = joinpath(root, file)
        isfile(src_file) && endswith(file, ".txt") && cp(src_file, joinpath(target, file); force=true)
    end
    for (file, header) in GTFS_HEADERS
        target_file = joinpath(target, file)
        isfile(target_file) || write(target_file, header)
    end
end

function extract_zip(zip_path::AbstractString, target::AbstractString)
    temp_dir = mktempdir(dirname(target))
    try
        sevenzip = Sys.which("7za")
        unzip = Sys.which("unzip")
        if sevenzip !== nothing
            run(Cmd([sevenzip, "x", zip_path, "-o$temp_dir", "-y"]))
        elseif unzip !== nothing
            run(Cmd([unzip, "-q", zip_path, "-d", temp_dir]))
        else
            error("Need 7za or unzip to stage zipped GTFS source: $zip_path")
        end
        copy_gtfs_dir(temp_dir, target)
    finally
        rm(temp_dir; recursive=true, force=true)
    end
end

function stage_source!(name::AbstractString, src::AbstractString, data_dir::AbstractString; replace_existing::Bool=false)
    target = joinpath(data_dir, "source=$name")
    if isdir(target)
        replace_existing || error("Staged source already exists: $target. Pass --replace-sources to replace it.")
        rm(target; recursive=true, force=true)
    end
    mkpath(dirname(target))
    if isdir(src)
        copy_gtfs_dir(src, target)
    elseif isfile(src) && (endswith(lowercase(src), ".zip") || endswith(lowercase(src), ".gtfs"))
        extract_zip(src, target)
    else
        error("Source must be a GTFS directory or zip file: $src")
    end
    println(">>> Staged source=$name from $src")
end

function write_csv(path::AbstractString, header::AbstractString, rows::Vector{String})
    if isempty(rows)
        write(path, header)
    else
        write(path, header * join(rows, "\n") * "\n")
    end
end

function generate_fantasy_fixture!(data_dir::AbstractString; replace_existing::Bool=false)
    target = joinpath(data_dir, "source=fixture_fantasy")
    if isdir(target)
        replace_existing || error("Fixture already exists: $target. Pass --replace-sources to replace it.")
        rm(target; recursive=true, force=true)
    end
    mkpath(target)

    write_csv(joinpath(target, "agency.txt"), GTFS_HEADERS["agency.txt"], [
        "toy,Toy Transit,https://example.invalid,Europe/Paris,,,,",
    ])
    write_csv(joinpath(target, "routes.txt"), GTFS_HEADERS["routes.txt"], [
        "r1,toy,R1,Fixture Route 1,,2,,,,,,",
        "r2,toy,R2,Fixture Route 2,,2,,,,,,",
    ])
    write_csv(joinpath(target, "trips.txt"), GTFS_HEADERS["trips.txt"], [
        "r1,svc_monday,r1_m_1,R1 Monday 1,,0,,,0,0",
        "r1,svc_monday,r1_m_2,R1 Monday 2,,0,,,0,0",
        "r1,svc_tuesday,r1_t_1,R1 Tuesday 1,,0,,,0,0",
        "r2,svc_monday,r2_m_1,R2 Monday 1,,0,,,0,0",
        "r2,svc_tuesday,r2_t_1,R2 Tuesday 1,,0,,,0,0",
        "r2,svc_tuesday,r2_t_2,R2 Tuesday 2,,0,,,0,0",
    ])
    write_csv(joinpath(target, "stops.txt"), GTFS_HEADERS["stops.txt"], [
        "a,,Alpha,,48.8566,2.3522,,,,,Europe/Paris,,,",
        "b,,Beta,,48.9000,2.4500,,,,,Europe/Paris,,,",
        "c,,Gamma,,48.7000,2.1000,,,,,Europe/Paris,,,",
        "d,,Delta,,48.6000,2.0000,,,,,Europe/Paris,,,",
    ])
    write_csv(joinpath(target, "calendar.txt"), GTFS_HEADERS["calendar.txt"], [
        "svc_monday,1,0,0,0,0,0,0,20260105,20260105",
        "svc_tuesday,0,1,0,0,0,0,0,20260106,20260106",
    ])
    write_csv(joinpath(target, "calendar_dates.txt"), GTFS_HEADERS["calendar_dates.txt"], String[])
    write_csv(joinpath(target, "shapes.txt"), GTFS_HEADERS["shapes.txt"], [
        "s1,48.8566,2.3522,1,0",
        "s1,48.9000,2.4500,2,10",
        "s2,48.7000,2.1000,1,0",
        "s2,48.6000,2.0000,2,10",
    ])
    write_csv(joinpath(target, "stop_times.txt"), GTFS_HEADERS["stop_times.txt"], [
        "r1_m_1,2026-01-05 08:00:00,2026-01-05 08:00:00,a,1,,,,,,0,1,",
        "r1_m_1,2026-01-05 08:30:00,2026-01-05 08:30:00,b,2,,,,,,10,1,",
        "r1_m_2,2026-01-05 09:00:00,2026-01-05 09:00:00,a,1,,,,,,0,1,",
        "r1_m_2,2026-01-05 09:30:00,2026-01-05 09:30:00,b,2,,,,,,10,1,",
        "r1_t_1,2026-01-06 08:00:00,2026-01-06 08:00:00,a,1,,,,,,0,1,",
        "r1_t_1,2026-01-06 08:30:00,2026-01-06 08:30:00,b,2,,,,,,10,1,",
        "r2_m_1,2026-01-05 10:00:00,2026-01-05 10:00:00,c,1,,,,,,0,1,",
        "r2_m_1,2026-01-05 10:30:00,2026-01-05 10:30:00,d,2,,,,,,10,1,",
        "r2_t_1,2026-01-06 10:00:00,2026-01-06 10:00:00,c,1,,,,,,0,1,",
        "r2_t_1,2026-01-06 10:30:00,2026-01-06 10:30:00,d,2,,,,,,10,1,",
        "r2_t_2,2026-01-06 11:00:00,2026-01-06 11:00:00,c,1,,,,,,0,1,",
        "r2_t_2,2026-01-06 11:30:00,2026-01-06 11:30:00,d,2,,,,,,10,1,",
    ])
    println(">>> Generated fantasy fixture at $target")
end

function stage_inputs!(cfg::Config, data_dir::AbstractString)
    if cfg.fixture !== nothing
        lowercase(cfg.fixture) == "fantasy" || error("Unknown fixture '$(cfg.fixture)'; only fantasy exists")
        generate_fantasy_fixture!(data_dir; replace_existing=cfg.replace_sources)
    end

    for (name, src) in cfg.sources
        stage_source!(name, resolve_source_path(src, cfg.data_root), data_dir; replace_existing=cfg.replace_sources)
    end

    if cfg.download_transitous
        grabber_script = joinpath(@__DIR__, "grabber.sh")
        mkpath(data_dir)
        println(">>> Downloading Transitous via grabber.sh. This is intentionally opt-in.")
        run(setenv(`bash $grabber_script $data_dir`, "DATA_ROOT" => cfg.data_root))
    end
end

function output_prefix_for(raw_prefix::AbstractString, mode::AbstractString)
    mode == "fantasy" && return raw_prefix
    mode == "real" && return "$(raw_prefix)real_"
    error("Unknown mode: $mode")
end

function main()
    cfg = parse_args(ARGS)
    dstr = date_str(cfg.target_date)
    data_dir = joinpath(cfg.data_root, date_dir_name(cfg.target_date))
    raw_prefix = "transitous_everything_$(dstr)_"

    stage_inputs!(cfg, data_dir)

    input_glob = cfg.input_root === nothing ? maybe_source_glob(data_dir) : maybe_source_glob(cfg.input_root)
    if cfg.input_root === nothing && !isdir(data_dir)
        error("No input data found at $data_dir. Use --source, --fixture fantasy, --input-root, or explicit --download-transitous.")
    end

    temp_dir = joinpath(@__DIR__, "temp", dstr)
    mkpath(temp_dir)

    base_vars = Dict(
        "RAW_PREFIX" => raw_prefix,
        "INPUT_GLOB" => sql_escape(input_glob),
        "MIN_DATE" => string(cfg.min_date),
        "MAX_DATE" => string(cfg.max_date),
        "STOP_UUIDS_TABLE" => "$(raw_prefix)stop_uuids",
    )

    raw_sql = render_template("01_raw.sql", base_vars, joinpath(temp_dir, "01_raw.sql"))
    run_sql_file(raw_sql, cfg.execute)

    if cfg.execute
        include("stop_uuid_generator.jl")
        conn = con()
        generate_uuids(conn, "$(raw_prefix)stops", "$(raw_prefix)stop_uuids")
    else
        println(">>> Dry run: stop UUID table would be $(raw_prefix)stop_uuids")
    end

    for mode in cfg.modes
        output_prefix = output_prefix_for(raw_prefix, mode)
        mode_vars = copy(base_vars)
        mode_vars["OUTPUT_PREFIX"] = output_prefix

        selector_template = mode == "fantasy" ? "02_select_fantasy.sql" : "02_select_real.sql"
        selector_sql = render_template(selector_template, mode_vars, joinpath(temp_dir, "02_select_$(mode).sql"))
        post_sql = render_template("03_postprocess.sql", mode_vars, joinpath(temp_dir, "03_postprocess_$(mode).sql"))

        run_sql_file(selector_sql, cfg.execute)
        run_sql_file(post_sql, cfg.execute)
    end

    println(">>> Orchestration complete. SQL is in $temp_dir")
end

main()
