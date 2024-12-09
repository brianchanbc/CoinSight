CREATE EXTERNAL TABLE brianchan_crypto_prices_minimal_hbase (
     pair_date STRING,
     pair STRING,
     price_date DATE,
     close STRING,
     volume STRING
)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES (
        'hbase.columns.mapping' = ':key,cf:pair,cf:price_date,cf:close,cf:volume'
        )
    TBLPROPERTIES ('hbase.table.name' = 'brianchan_crypto_prices_minimal_hbase');

INSERT INTO TABLE brianchan_crypto_prices_minimal_hbase
SELECT
    CONCAT(pair, '_', price_date) AS pair_date,
    pair,
    price_date,
    close,
    volume
FROM
    (
        SELECT
            pair,
            price_date,
            close,
            volume,
            ROW_NUMBER() OVER (PARTITION BY pair, price_date ORDER BY price_time DESC) as row_num
        FROM brianchan_crypto_prices
    ) tmp
WHERE row_num = 1
ORDER BY pair, price_date;

-- select COUNT(*) from brianchan_crypto_prices_minimal_hbase;
-- 431690