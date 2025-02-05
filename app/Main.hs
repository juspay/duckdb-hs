{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import DuckDB
import Options.Applicative
import Data.HashMap.Strict as HM
import Conduit
import Data.Conduit ((.|), runConduit, ConduitT, await, leftover)
import qualified Data.Conduit.Combinators as Conduit
import Data.Aeson

main :: IO ()
main = do
  print "Testing duckdb ffi"
  res <- duckdbOpen (Nothing) (Just [("threads", "8"), ("max_memory", "12GB")])
  print "Running queries"

  -- duckdbQuery res "INVALID SQL STATEMENT;"
  -- duckdbQuery res "LOAD httpfs;"
  -- duckdbQuery res "INSTALL aws;"
  -- duckdbQuery res "LOAD aws;"
  -- duckdbQuery res "CALL load_aws_credentials();"

  duckdbConfigureAWS res
  duckdbQuery res "PRAGMA memory_limit='12GB';"
  duckdbQuery res "PRAGMA temp_directory='/tmp/large-disk';"
  duckdbQuery res "COPY ( SELECT * FROM read_json_auto('s3://jp-k8s-mum-logs-bd/txn/logs/2025/02/04/10/06-ip-10-8-148-70.ap-south-1.compute.internal-8b0020ab76cea75.gz', union_by_name = true, sample_size=1000) ORDER BY date_created, merchant_id, txn_uuid ) TO 's3://juspay-docs/test-duckdb-parquet/txn/' (FORMAT PARQUET, PARTITION_BY (date_created_year_string, date_created_month_string, date_created_day_string, date_created_hour_string), overwrite_or_ignore);"
  -- duckdbQuery res "INSERT INTO INTTf VALUES (3,'hello', '2025-01-06 11:30:00.123456789'), (5,'hii', '1992-09-20 11:30:00.123456789'),(7, NULL, '1992-09-20 11:30:00.123456789');"

  -- runConduit $ do
  --           (duckdbQueryWithResponse res "SELECT count(1) as c FROM '/Users/rahulsingh/Documents/duckdb-hs/tests/artifacts/test.parquet'")
  --           .| Conduit.map (encode)
  --           .| Conduit.map (BS.toStrict)
  --           .| Conduit.map (<> "\n")
  --           .| Conduit.stdout
  duckdbClose res
  pure ()
-- ( ( ( ( ( ( merchant_id IN ( 'goindigo' ) ) AND ( actual_payment_status IN ( 'AUTHORIZATION_FAILED' ) ) ) AND ( payment_gateway IN ( 'RAZORPAY', 'CCAVENUE_V2' ) ) ) AND ( payment_instrument_group IN ( 'CREDIT CARD', 'DEBIT CARD', 'NET BANKING', 'WALLET', 'UPI' ) ) ) AND ( gateway_reference_id IN ( 'htl' ) ) ) ) AND
-- (duckdbQueryConduitRes res "SELECT * FROM 's3://bulk-download-row-binary/parquet/juspayonly/rowbinary/txn/2025/01/22/10/000001737541437.parquet';")
-- s3://bulk-download-row-binary/test/parquet/userdata1.parquet
-- SELECT ( ( IF ( ( SUM ( ( ( 1 ) * ( 1 ) ) ) >= 0 ), SUM ( ( ( 1 ) * ( 1 ) ) ), NULL ) AS total_volume ) AS total_volume ), payment_method_type FROM 's3://bulk-download-row-binary/parquet/juspayonly/rowbinary/txn/2025/01/22/10/000001737541437.parquet' WHERE ( ( ( ( ( ( merchant_id IN ( 'goindigo' ) ) AND ( actual_payment_status IN ( 'AUTHORIZATION_FAILED' ) ) ) AND ( payment_gateway IN ( 'RAZORPAY', 'CCAVENUE_V2' ) ) ) AND ( payment_instrument_group IN ( 'CREDIT CARD', 'DEBIT CARD', 'NET BANKING', 'WALLET', 'UPI' ) ) ) AND ( gateway_reference_id IN ( 'htl' ) ) ) AND ( payment_method_type IN ( 'CARD', 'UPI', 'NB', 'WALLET' ) ) ) AND ( txn_initiated < '2025-01-30 07:00:00' ) AND ( txn_initiated >= '2025-01-30 06:45:00' ) GROUP BY payment_method_type HAVING ( total_volume < 10 );