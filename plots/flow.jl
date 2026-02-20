using DataFrames, ImageFiltering, LinearAlgebra, CSV, Interpolations

PARIS_LAT = 48.8566
PARIS_LON = 2.3522

RESOLUTION = 0.1
GRID_WIDTH = round(Int, 360 / RESOLUTION)
GRID_HEIGHT = round(Int, 180 / RESOLUTION)

function haversine_dist(lat1, lon1, lat2, lon2)
    R = 6371.0
    φ1, φ2 = deg2rad(lat1), deg2rad(lat2)
    Δφ = deg2rad(lat2 - lat1)
    Δλ = deg2rad(lon2 - lon1)
    
    a = sin(Δφ/2)^2 + cos(φ1)*cos(φ2)*sin(Δλ/2)^2
    c = 2 * atan(sqrt(a), sqrt(1-a))
    return R * c
end

function coord_to_grid(lat, lon)
    x = round(Int, (lon + 180) / RESOLUTION) + 1
    y = round(Int, (lat + 90) / RESOLUTION) + 1
    # Clamp to ensure we don't go out of bounds
    x = clamp(x, 1, GRID_WIDTH)
    y = clamp(y, 1, GRID_HEIGHT)
    return x, y
end

function build_vector_field(edges_df::DataFrame)
    grid_U = zeros(Float64, GRID_HEIGHT, GRID_WIDTH)
    grid_V = zeros(Float64, GRID_HEIGHT, GRID_WIDTH)
    # grid_W = ones(Float64, GRID_HEIGHT, GRID_WIDTH)
    
    for row in eachrow(edges_df)
        dist_start = haversine_dist(row.start_lat, row.start_lon, PARIS_LAT, PARIS_LON)
        dist_finish = haversine_dist(row.finish_lat, row.finish_lon, PARIS_LAT, PARIS_LON)
        
        if dist_start > dist_finish
            lat1, lon1 = row.start_lat, row.start_lon
            lat2, lon2 = row.finish_lat, row.finish_lon
        else
            lat1, lon1 = row.finish_lat, row.finish_lon
            lat2, lon2 = row.start_lat, row.start_lon
        end
        
        dy = lat2 - lat1
        dx = lon2 - lon1
        
        if dx == 0 && dy == 0 continue end 
        
        len = sqrt(dx^2 + dy^2)
        u = (dx / len) * row.speed
        v = (dy / len) * row.speed
        
        x1, y1 = coord_to_grid(lat1, lon1)
        x2, y2 = coord_to_grid(lat2, lon2)
        
        steps = max(abs(x2 - x1), abs(y2 - y1))
        
        if steps == 0
            # Apply to specific cell
            grid_U[y1, x1] += u
            grid_V[y1, x1] += v
            # grid_W[y1, x1] += 1.0
        else
            x_inc = (x2 - x1) / steps
            y_inc = (y2 - y1) / steps
            
            curr_x, curr_y = Float64(x1), Float64(y1)
            for _ in 0:steps
                gx, gy = round(Int, curr_x), round(Int, curr_y)
                
                # only store if greater than current cell speed
                ours = sqrt(u^2 + v^2)
                theirs = sqrt(grid_U[gy, gx]^2 + grid_V[gy, gx]^2)
                (ours > 350) && continue # skip broken. doesn't change anything :(
                
                if (1 <= gx <= GRID_WIDTH) && (1 <= gy <= GRID_HEIGHT) && (ours > theirs)
                    grid_U[gy, gx] = u
                    grid_V[gy, gx] = v
                    # grid_W[gy, gx] += 1.0 # Add weight to average later
                end
                
                curr_x += x_inc
                curr_y += y_inc
            end
        end
    end
    
    # grid_U ./= grid_W
    # grid_V ./= grid_W
    
    # Kernel Smoothing 
    # smoothed_U = imfilter(grid_U, Kernel.gaussian(3.0))
    # smoothed_V = imfilter(grid_V, Kernel.gaussian(3.0))
    
    # return smoothed_U, smoothed_V
    return grid_U, grid_V
end

edges_df = CSV.read("trains_a_vitesse.csv", DataFrame)
smoothed_U, smoothed_V = build_vector_field(edges_df)

interp_U = linear_interpolation((1:GRID_HEIGHT, 1:GRID_WIDTH), smoothed_U)
interp_V = linear_interpolation((1:GRID_HEIGHT, 1:GRID_WIDTH), smoothed_V)

function get_flow(x, y)
    gx = (x + 180) / RESOLUTION + 1
    gy = (y + 90) / RESOLUTION + 1
    
    gx = clamp(gx, 1.0, Float64(GRID_WIDTH))
    gy = clamp(gy, 1.0, Float64(GRID_HEIGHT))
    
    return Point2f(interp_U(gy, gx), interp_V(gy, gx))
end


# fig = Figure(size = (1600, 800), backgroundcolor = :black)

# x_limits = -15..35
# y_limits = 34..60

# ax = Axis(fig[1, 1], 
#     title = "European Rail Flow Towards Paris", 
#     backgroundcolor = :black,
#     aspect = DataAspect(),
#     limits = ((-15, 35), (34, 60)) # This forces the axis to zoom in
# )

# streamplot!(ax, get_flow, x_limits, y_limits, 
#     colormap = :plasma,
#     gridsize = (100*4, 100*4), # Increase density since we are zoomed in
#     linewidth = 0.5,
#     arrow_size = 0.0,
# )

# hidedecorations!(ax) 
# hidespines!(ax)

# resize_to_layout!(fig)

# save("pls.png", fig)
# it's a cool start but i think it would be nicer on a map

using JSON3, Statistics, CairoMakie

SPEED_THRESHOLD2 = 2.0 

n = 12
f_ax_plt = streamplot(get_flow, -180..180, -90..90,
    gridsize = (70*n, 70*n), 
    density = 0.8, 
    stepsize = 0.05
)
plt = f_ax_plt.plot
raw_points = plt.plots[1][1][]

all_features = []
current_line_points = Vector{Vector{Float64}}()
last_speed = -1.0

for p in raw_points
    if isnan(p[1])
        empty!(current_line_points)
        last_speed = -1.0
        continue
    end

    this_p = [Float64(p[1]), Float64(p[2])]
    this_speed = sqrt(mapreduce(x->x^2, +, get_flow(p[1], p[2])))

    if isempty(current_line_points)
        push!(current_line_points, this_p)
        last_speed = this_speed
    else
        if abs(this_speed - last_speed) > SPEED_THRESHOLD2
            if length(current_line_points) >= 2
                push!(all_features, Dict(
                    "type" => "Feature",
                    "geometry" => Dict("type" => "LineString", "coordinates" => copy(current_line_points)),
                    "properties" => Dict("speed" => min(round(last_speed, digits=1), 350)) # hack
                ))
            end
            
            last_p = current_line_points[end]
            empty!(current_line_points)
            push!(current_line_points, last_p) 
            push!(current_line_points, this_p)
            last_speed = this_speed
        else
            # Speed is stable, just keep adding points to this segment
            push!(current_line_points, this_p)
        end
    end
end

geojson_data = Dict("type" => "FeatureCollection", "features" => all_features)
open("rail_flow_segmented3.geojson", "w") do io
    JSON3.write(io, geojson_data)
end

println("Created $(length(all_features)) segments. Speed gradients preserved")

# todo: add some kind of smoothing maybe?
# change target city?
# debug & fix the >350kph speeds
