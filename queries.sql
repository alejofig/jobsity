-- From the two most commonly appearing regions, which is the latest datasource?
with b as(
    select
        max(datetime) as max_date,
        region
    from
        trips
    where
        region in(
            with a as(
                select
                    count(*),
                    region
                from
                    trips
                group by
                    2
                order by
                    1 desc
                limit
                    2
            )
            select
                region
            from
                a
        )
    group by
        2
)
select
    datasource
from
    b
    inner join trips on trips.datetime = b.max_date
    and trips.region = b.region 


-- What regions has the "cheap_mobile" datasource appeared in?

select distinct region from trips where datasource = 'cheap_mobile'