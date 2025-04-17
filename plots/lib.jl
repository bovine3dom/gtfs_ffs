#!/bin/julia

using ClickHouse, StatsBase
CLICKHOUSE_USER = get(ENV,"CLICKHOUSE_USER", "admin")
CLICKHOUSE_DB = get(ENV,"CLICKHOUSE_DB", "default")
CLICKHOUSE_PASSWORD = get(ENV,"CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_NATIVE_PORT = parse(Int,get(ENV,"CLICKHOUSE_NATIVE_PORT", "9000"))
CLICKHOUSE_HOST = get(ENV,"CLICKHOUSE_HOST", "localhost")
MAX_CLICKHOUSE_QUERY_SIZE = 10_000
H3_PERMITTED_RESOLUTIONS = [11, 9, 7]

"Connect to the ClickHouse server"
con() = connect(CLICKHOUSE_HOST, CLICKHOUSE_NATIVE_PORT; database=CLICKHOUSE_DB, username=CLICKHOUSE_USER, password = CLICKHOUSE_PASSWORD)

"Convert an array of strings to a ClickHouse tuple"
julia2clickhouse(v) = begin
    "('" * join(v, "', '", ) * "')"
end

"Convert a dataframe into a dict for use with ClickHouse.insert"
df2dict(df) = Dict(n => c for (n,c) in Iterators.zip(Symbol.(names(df)), eachcol(df)))

"Get column names => type for ClickHouse from dataframe"
chtypes(df) = Dict(zip(names(df), maybeNullableElString.(eachcol(df))))

toChArrayStr(t) = begin
    if (t <: AbstractArray)
        return "Array($(eltype(t)))"
    end
    return string(t)
end

maybeNullableElString(column) = begin
    t = eltype(column)
    return if (nonmissingtype(t) == t)
        toChArrayStr(t)
    else
        "Nullable($(toChArrayStr(nonmissingtype(t))))"
    end
end

"Convert dict of type strings to ClickHouse table specification"
tablespec(types) = "(" * join(("$k $v" for (k,v) in types), ", ") * ")"

"Make a function ignore all missing data"
headinsand(f, x) = begin
    x = skipmissing(x)
    isempty(x) && return missing
    f(x)
end
headinsand(f) = x -> headinsand(f, x) # or Base.Fix1(headinsand, f)

# can use groupby via combine(groupby(df, :group), d -> addquantiles!(d, :whatever))
"Add [column]_quantile to a dataframe. If jiggle=true, no ties are allowed"
addquantiles!(df, column; jiggle=false) = begin
    if (!jiggle) 
        raw = ecdf(df[!, column]).(df[!, column])
        raw = raw .- minimum(raw)
        raw = raw ./ maximum(raw)
        return df[!, Symbol(string(column) * "_quantile")] = raw
    end
    l = size(df,1)
    tdf = copy(df[!, [column]])
    tdf.id = 1:l
    sort!(tdf, column)
    tdf.q = (1:l)./l
    sort!(tdf, :id)
    return df[!, Symbol(string(column) * "_quantile")] = tdf.q
end

"Get actual-value labels for quantiles. Fudge 0/1 to 0.0001/0.9999"
quantilelabels(array; middle = [0.2, 0.4, 0.6, 0.8], nonlinear=false, n=8) = begin
    !nonlinear && return ([0, middle..., 1], round.(quantile.(Ref(array), [0.0001, middle..., 0.9999]), sigdigits=2))
    f = ecdf(array)
    vs = range(quantile(array, 0.0001), quantile(array, 0.9999), n)
    qs = f.(vs)
    return (qs, round.(vs, sigdigits=2))
end

"Stick columns at the front for easy viewing"
atfront(df, cols) = DataFrames.select(df, cols..., Not(cols...))
