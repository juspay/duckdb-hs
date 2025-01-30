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
  res <- duckdbOpenAndConnect (Nothing) (Nothing :: Maybe [(String, String)])
  print "Running queries"
  
  -- duckdbQuery res "INSTALL httpfs;"
  -- duckdbQuery res "LOAD httpfs;"
  -- duckdbQuery res "INSTALL aws;"
  -- duckdbQuery res "LOAD aws;"
  -- duckdbQuery res "CALL load_aws_credentials();"

  duckdbConfigureAWS res
  -- duckdbQuery res "CREATE TABLE IF NOT EXISTS INTTf (i INTEGER, j VARCHAR, k TIMESTAMP);"
  -- duckdbQuery res "INSERT INTO INTTf VALUES (3,'hello', '2025-01-06 11:30:00.123456789'), (5,'hii', '1992-09-20 11:30:00.123456789'),(7, NULL, '1992-09-20 11:30:00.123456789');"
  
  runConduit $ do
            (duckdbQueryWithResponse res "SELECT * FROM 's3://bulk-download-row-binary/test/parquet/userdata1.parquet';")
            .| Conduit.map (encode)
            .| Conduit.map (BS.toStrict)
            .| Conduit.map (<> "\n")
            .| Conduit.stdout
  duckdbDisconnectAndClose res
  pure ()

-- (duckdbQueryConduitRes res "SELECT * FROM 's3://bulk-download-row-binary/parquet/juspayonly/rowbinary/txn/2025/01/22/10/000001737541437.parquet';")
-- s3://bulk-download-row-binary/test/parquet/userdata1.parquet