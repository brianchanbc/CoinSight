CREATE EXTERNAL TABLE brianchan_crypto_prices_full_hbase (
    pair_date_time STRING,
    pair STRING,
    price_date DATE,
    price_time STRING,
    full_timestamp STRING,
    open DOUBLE,
    close DOUBLE,
    high DOUBLE,
    low DOUBLE,
    volume DOUBLE
)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES (
        'hbase.columns.mapping' = ':key,cf:pair,cf:price_date,cf:price_time,cf:full_timestamp,cf:open,cf:close,cf:high,cf:low,cf:volume'
        )
    TBLPROPERTIES ('hbase.table.name' = 'brianchan_crypto_prices_full_hbase');

INSERT INTO TABLE brianchan_crypto_prices_full_hbase
SELECT
    CONCAT(pair, '_', price_date, '_', price_time) AS pair_date_time,
    pair,
    price_date,
    price_time,
    full_timestamp,
    open,
    close,
    high,
    low,
    volume
FROM brianchan_crypto_prices;