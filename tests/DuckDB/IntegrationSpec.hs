{-# LANGUAGE OverloadedStrings #-}

module DuckDB.IntegrationSpec where

import Test.Hspec
import Control.Monad.IO.Class (liftIO)
import DuckDB
import qualified Data.Text as T
import Conduit
import qualified Data.Conduit.Combinators as C
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.ByteString as BS
import Data.Aeson (decode, Value(..), (.:), encode)
import Control.Exception (evaluate)
import Control.Monad (void)
import Paths_duckdb_hs (getDataFileName)

spec :: Spec
spec = do
  describe "DuckDB-S3 Integration Tests" $ do
    it "opens and closes the DuckDB database successfully" $ do
      db <- duckdbOpen Nothing (Nothing :: Maybe [(String, String)])
      duckdbClose db

    it "executes a simple CREATE TABLE and INSERT query" $ do
      db <- duckdbOpen Nothing (Nothing :: Maybe [(String, String)])
      duckdbQuery db "CREATE TABLE test_table (id INTEGER, name VARCHAR);"
      duckdbQuery db "INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob');"
      duckdbClose db

    it "fetches data using SELECT query" $ do
      db <- duckdbOpen Nothing (Nothing :: Maybe [(String, String)])
      duckdbQuery db "CREATE TABLE test_table (id INTEGER, name VARCHAR);"
      duckdbQuery db "INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob');"
    
      result <- (runConduit $
                  duckdbQueryWithResponse db "SELECT * FROM test_table;"
                  .| C.map (encode)
                  .| C.map (BS.toStrict)
                  .| C.sinkList :: IO [BS.ByteString])
      result `shouldSatisfy` (\r -> any ("Alice" `BS.isInfixOf`) r && any ("Bob" `BS.isInfixOf`) r)
      duckdbClose db

    -- it "configures AWS plugins successfully" $ do
    --   db <- duckdbOpen Nothing (Nothing :: Maybe [(String, String)])
    --   duckdbConfigureAWS db
    --   duckdbClose db

    it "handles invalid queries with errors" $ do
      db <- duckdbOpen Nothing (Nothing :: Maybe [(String, String)])
      duckdbQuery db "INVALID SQL STATEMENT" `shouldThrow` anyIOException
      duckdbClose db

    it "queries data from S3 Parquet files correctly" $ do
      db <- duckdbOpen Nothing (Nothing :: Maybe [(String, String)])
      fn <- getDataFileName "tests/artifacts/test.parquet"
      let query = "SELECT COUNT(1) as c FROM '" <> fn <>"'"
      result <- (runConduit $
                  duckdbQueryWithResponse db query
                  .| C.map (encode)
                  .| C.map (BS.toStrict)
                  .| C.sinkList :: IO [BS.ByteString])
      duckdbClose db
      result `shouldBe` ["{\"c\":1000}"]

    it "queries data from S3 Parquet files and checking the length of the stream" $ do
      db <- duckdbOpen Nothing (Nothing :: Maybe [(String, String)])
      fn <- getDataFileName "tests/artifacts/test.parquet"
      let query = "SELECT * FROM '" <> fn <>"'"
      result <- (runConduit $
                  duckdbQueryWithResponse db query
                  .| C.map (encode)
                  .| C.map (BS.toStrict)
                  .| C.length)
      duckdbClose db
      result `shouldBe` 1000
