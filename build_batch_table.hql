-- Note: Create this temporary external table first to read the raw CSV
CREATE EXTERNAL TABLE IF NOT EXISTS brianchan_crypto_prices_raw (
     pair STRING,
     `time` STRING,
     open DOUBLE,
     close DOUBLE,
     high DOUBLE,
     low DOUBLE,
     volume DOUBLE
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/brianchan/datalake/combined_data'
    TBLPROPERTIES ("skip.header.line.count"="1");

-- Create the external table with readable dates
CREATE EXTERNAL TABLE IF NOT EXISTS brianchan_crypto_prices
    STORED AS TEXTFILE
AS
SELECT
    pair,
    to_date(from_unixtime(cast(cast(`time` as BIGINT) / 1000 as BIGINT))) as price_date,
    from_unixtime(cast(cast(`time` as BIGINT) / 1000 as BIGINT), 'HH:mm:ss') as price_time,
    from_unixtime(cast(cast(`time` as BIGINT) / 1000 as BIGINT)) as full_timestamp,
    open,
    close,
    high,
    low,
    volume
FROM (
        SELECT * FROM brianchan_crypto_prices_raw
        WHERE pair != 'pair'
    ) raw;