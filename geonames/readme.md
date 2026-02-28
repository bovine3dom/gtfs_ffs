```bash
wget https://download.geonames.org/export/dump/cities500.zip
7za x cities500.zip
sed -i '1i geonameid\tname\tasciiname\talternatenames\tlatitude\tlongitude\tfeature_class\tfeature_code\tcountry_code\tcc2\tadmin1_code\tadmin2_code\tadmin3_code\tadmin4_code\tpopulation\televation\tdem\ttimezone\tmodification_date' cities500.txt
mv cities500.txt cities500.tsv
scp cities500.tsv ...
```

```clickhouse
create table public_geonames
engine = MergeTree
order by (h3, population)
as
select *, geoToH3(latitude, longitude, 8) h3 from file('chungus/cities500.tsv', 'TSVWithNames', '
    geonameid UInt32,
    name String,
    asciiname String,
    alternatenames String,
    latitude Float64,
    longitude Float64,
    feature_class LowCardinality(String),
    feature_code LowCardinality(String),
    country_code LowCardinality(String),
    cc2 LowCardinality(String),
    admin1_code LowCardinality(String),
    admin2_code LowCardinality(String),
    admin3_code LowCardinality(String),
    admin4_code LowCardinality(String),
    population UInt32,
    elevation Int16,
    dem Int16,
    timezone LowCardinality(String),
    modification_date Date
')
```


```
select geonameid, any(name), any(country_code), sum(p.population) from (
    select geonameid, name, country_code, p.population, arrayJoin(h3kRing(g.h3, 2)) h3 from public_geonames g
    inner join public_kontur_population_20231101 p on g.h3 = p.h3
    where country_code = 'GB'
    and population > 10
    order by geonameid
    limit 2000
)
group by geonameid
order by geonameid
limit 10
```

... but why am i doing this anyway? was it just for vanity names? big h3 -> biggest town in that h3 is probably a useful table
