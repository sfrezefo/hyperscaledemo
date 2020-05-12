# Real-time data with Azure Database for PostgreSQL Hyperscale

## Abstracts
This workshop is a simplified version of a very complete workshop :

[MCW-Real-time-data-with-Azure-Database-for-PostgreSQL-Hyperscale]( https://github.com/microsoft/MCW-Real-time-data-with-Azure-Database-for-PostgreSQL-Hyperscale)

The main challenge is to analyze user clickstream data, online ad performance, and other marketing campaigns at scale, and to provide insights to the marketing team in real-time.
The aim is to learn how to use advanced features of the managed PostgreSQL PaaS service on Azure to make a database more scalable and able to handle the rapid ingest of streaming data while simultaneously generating and serving pre-aggregated data for reports.
At the end of this workshop, you will be better able to implement a highly scalable, managed open source database solution that can simultaneously handle real-time data and roll-up and serve data.

## Target audience

- Database Administrator
- Data Engineer
- Data Scientist
- Database Developer
- Solution Architect

## Azure services and related products

- Azure Database for PostgreSQL
- Azure Cloud Shell
- pgAdmin

## Getting Started

 we are sharding each of the tables on customer_id column. 
 The sharding logic is handled for you by the Hyperscale server group (enabled by Citus), allowing you to horizontally scale your database across multiple managed Postgres servers. 
 This provides you with multi-tenancy because the data is sharded by the same Tenant ID (customer_id).
  Because we are sharding on the same ID for our raw events table and rollup tables, our data stored in both types of table are automatically co-located for us by Citus. Furthermore, 
 this means that aggregations can be performed locally without crossing network boundaries when we insert our events data into the rollup tables. 

Create the events raw table to capture every clickstream event. 
This table is partitioned by event_time since we are using it to store time series data. 
The script you execute to create the schema creates a partition every 5 minutes, using pg_partman.

```Shell
psql --host=postgreshypersg-c.postgres.database.azure.com \
   --variable=sslmode=require --port=5432  --dbname=citus --username=citus  -W
```

```sql
CREATE TABLE events(
    event_id serial,
    event_time timestamptz default now(),
    customer_id bigint,
    event_type text,
    country text,
    browser text,
    device_id bigint,
    session_id bigint
)
PARTITION BY RANGE (event_time);
```
partitioning
```sql
--Create 5-minutes partitions
SELECT partman.create_parent('public.events', 'event_time', 'native', '5 minutes');
UPDATE partman.part_config SET infinite_time_partitions = true;
```
rollup tables
```sql
create two rollup tables for storing aggregated data pulled from the raw events table. 
Later, you will create rollup functions and schedule them to run periodically.
CREATE TABLE rollup_events_5min (
     customer_id bigint,
     event_type text,
     country text,
     browser text,
     minute timestamptz,
     event_count bigint,
     device_distinct_count hll,
     session_distinct_count hll,
     top_devices_1000 jsonb
 );
 CREATE UNIQUE INDEX rollup_events_5min_unique_idx ON rollup_events_5min(customer_id,event_type,country,browser,minute);
 SELECT create_distributed_table('rollup_events_5min','customer_id');
```
rollup 1h
```sql
 CREATE TABLE rollup_events_1hr (
     customer_id bigint,
     event_type text,
     country text,
     browser text,
     hour timestamptz,
     event_count bigint,
     device_distinct_count hll,
     session_distinct_count hll,
     top_devices_1000 jsonb
 );
 CREATE UNIQUE INDEX rollup_events_1hr_unique_idx ON rollup_events_1hr(customer_id,event_type,country,browser,hour);
 SELECT create_distributed_table('rollup_events_1hr','customer_id');
```
distribute
```sql
 --shard the events table as well
 SELECT create_distributed_table('events','customer_id');

```
metadata about rollups
```sql
 CREATE TABLE rollups (
    name text primary key,
    event_table_name text not null,
    event_id_sequence_name text not null,
    last_aggregated_id bigint default 0
);

```
function to find start and end position to compute rollups
```sql
CREATE OR REPLACE FUNCTION incremental_rollup_window(rollup_name text, OUT window_start bigint, OUT window_end bigint)
RETURNS record
LANGUAGE plpgsql
AS $function$
DECLARE
    table_to_lock regclass;
BEGIN
    /*
    * Perform aggregation from the last aggregated ID + 1 up to the last committed ID.
    * We do a SELECT .. FOR UPDATE on the row in the rollup table to prevent
    * aggregations from running concurrently.
    */
    SELECT event_table_name, last_aggregated_id+1, pg_sequence_last_value(event_id_sequence_name)
    INTO table_to_lock, window_start, window_end
    FROM rollups
    WHERE name = rollup_name FOR UPDATE;

    IF NOT FOUND THEN
        RAISE 'rollup ''%'' is not in the rollups table', rollup_name;
    END IF;

    IF window_end IS NULL THEN
        /* sequence was never used */
        window_end := 0;
        RETURN;
    END IF;

    /*
    * Play a little trick: We very briefly lock the table for writes in order to
    * wait for all pending writes to finish. That way, we are sure that there are
    * no more uncommitted writes with an identifier lower or equal to window_end.
    * By throwing an exception, we release the lock immediately after obtaining it
    * such that writes can resume.
    */
    BEGIN
        EXECUTE format('LOCK %s IN EXCLUSIVE MODE', table_to_lock);
        RAISE 'release table lock';
    EXCEPTION WHEN OTHERS THEN
    END;

    /*
    * Remember the end of the window to continue from there next time.
    */
    UPDATE rollups SET last_aggregated_id = window_end WHERE name = rollup_name;
END;
$function$;
```
enter initial position for the 2 rollups we compute
```sql
-- Entries for the rollup tables so that they are getting tracked in incremental rollup process.
INSERT INTO rollups (name, event_table_name, event_id_sequence_name)
VALUES ('rollup_events_5min', 'events','events_event_id_seq');

INSERT INTO rollups (name, event_table_name, event_id_sequence_name)
VALUES ('rollup_events_1hr', 'events','events_event_id_seq');

```
function that actually compute the 5 minutes rollup
```sql
CREATE OR REPLACE FUNCTION five_minutely_aggregation(OUT start_id bigint, OUT end_id bigint)
RETURNS record
LANGUAGE plpgsql
AS $function$
BEGIN
    /* determine which page views we can safely aggregate */
    SELECT window_start, window_end INTO start_id, end_id
    FROM incremental_rollup_window('rollup_events_5min');

    /* exit early if there are no new page views to aggregate */
    IF start_id > end_id THEN RETURN; END IF;

    /* aggregate the page views, merge results if the entry already exists */
    INSERT INTO rollup_events_5min
        SELECT customer_id,
                event_type,
                country,
                browser,
                date_trunc('seconds', (event_time - TIMESTAMP 'epoch') / 300) * 300 + TIMESTAMP 'epoch' AS minute,
                count(*) as event_count,
                hll_add_agg(hll_hash_bigint(device_id)) as device_distinct_count,
                hll_add_agg(hll_hash_bigint(session_id)) as session_distinct_count,
                topn_add_agg(device_id::text) top_devices_1000
        FROM events WHERE event_id BETWEEN start_id AND end_id
        GROUP BY customer_id,event_type,country,browser,minute
        ON CONFLICT (customer_id,event_type,country,browser,minute)
        DO UPDATE
        SET event_count=rollup_events_5min.event_count+excluded.event_count,
            device_distinct_count = hll_union(rollup_events_5min.device_distinct_count, excluded.device_distinct_count),
            session_distinct_count= hll_union(rollup_events_5min.session_distinct_count, excluded.session_distinct_count),
            top_devices_1000 = topn_union(rollup_events_5min.top_devices_1000, excluded.top_devices_1000);
END;
$function$;

```
function that actually compute the one hour rollup
```sql
CREATE OR REPLACE FUNCTION hourly_aggregation(OUT start_id bigint, OUT end_id bigint)
RETURNS record
LANGUAGE plpgsql
AS $function$
BEGIN
    /* determine which page views we can safely aggregate */
    SELECT window_start, window_end INTO start_id, end_id
    FROM incremental_rollup_window('rollup_events_1hr');

    /* exit early if there are no new page views to aggregate */
    IF start_id > end_id THEN RETURN; END IF;

    /* aggregate the page views, merge results if the entry already exists */
    INSERT INTO rollup_events_1hr
        SELECT customer_id,
                event_type,
                country,
                browser,
                date_trunc('hour', event_time) as hour,
                count(*) as event_count,
                hll_add_agg(hll_hash_bigint(device_id)) as device_distinct_count,
                hll_add_agg(hll_hash_bigint(session_id)) as session_distinct_count,
                topn_add_agg(device_id::text) top_devices_1000
        FROM events WHERE event_id BETWEEN start_id AND end_id
        GROUP BY customer_id,event_type,country,browser,hour
        ON CONFLICT (customer_id,event_type,country,browser,hour)
        DO UPDATE
        SET event_count = rollup_events_1hr.event_count+excluded.event_count,
            device_distinct_count = hll_union(rollup_events_1hr.device_distinct_count,excluded.device_distinct_count),
            session_distinct_count = hll_union(rollup_events_1hr.session_distinct_count,excluded.session_distinct_count),
            top_devices_1000 = topn_union(rollup_events_1hr.top_devices_1000, excluded.top_devices_1000);
END;
$function$;
```
Schedule a periodic computation fof the rollups
```sql
SELECT cron.schedule('*/5 * * * *', 'SELECT five_minutely_aggregation();');
SELECT cron.schedule('*/5 * * * *', 'SELECT hourly_aggregation();');

```
we can force the computation of the rollup by explicitely calling the aggreagtion 
```sql
SELECT five_minutely_aggregation();
SELECT hourly_aggregation();
```
 Hyperscale clusters allow us to parallelize our aggregations across shards, 
 then perform a SELECT on a rollup for a particular customer from the dashboard, and have it automatically routed to the appropriate shard.

-- the total number of events and count of distinct devices in the last 15 minutes
```sql
SELECT sum(event_count) num_events, 
      ceil(hll_cardinality(hll_union_agg(device_distinct_count))) distinct_devices
FROM rollup_events_5min 
WHERE minute >=now()-interval '15 minutes' 
  AND minute <=now();
```

-- the count of distinct sessions over the past week:
```sql
SELECT sum(event_count) num_events,
        ceil(hll_cardinality(hll_union_agg(device_distinct_count))) distinct_devices
FROM rollup_events_1hr
WHERE hour >=date_trunc('day',now())-interval '7 days'
    AND hour <=now();

```
-- the trend of app usage in the past 2 days, broken down by hour:
```sql
SELECT hour,
        sum(event_count) event_count,
        ceil(hll_cardinality(hll_union_agg(device_distinct_count))) device_count,
        ceil(hll_cardinality(hll_union_agg(session_distinct_count))) session_count
FROM rollup_events_1hr
WHERE hour >=date_trunc('day',now())-interval '2 days'
    AND hour <=now()
GROUP BY hour;

```
-- the total number of events and count of distinct devices in the last 15 minutes by customer_id.
-- Remember, the data is sharded by tenant (Customer ID):
```sql
SELECT sum(event_count) num_events, 
    ceil(hll_cardinality(hll_union_agg(device_distinct_count))) distinct_devices
FROM rollup_events_5min 
WHERE minute >=now()-interval '15 minutes' 
    AND minute <=now() 
    AND customer_id=1;

```
-- top devices in the past 30 minutes for customer 2:
```sql
SELECT (topn(topn_union_agg(top_devices_1000), 10)).item device_id
FROM rollup_events_5min
WHERE minute >=date_trunc('day',now())-interval '30 minutes'
    AND minute <=now()
    AND customer_id=2;
```
    
## Authors

* **Serge Frezefond** - [PurpleBooth](https://linnkedin.com/serge.frezefond)
