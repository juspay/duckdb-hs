{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module DuckDB (duckdbOpen, duckdbConfigureAWS, duckdbQueryWithResponse, duckdbQuery, duckdbClose)
where

import Foreign
    ( Word8,
      Ptr,
      Storable(poke),
      plusPtr,
      newForeignPtr,
      finalizerFree,
      malloc,
      finalizeForeignPtr,
      touchForeignPtr,
      withForeignPtr,
      FinalizerPtr,
      ForeignPtr )
import Foreign.C.Types ( CSize, CInt )
import Foreign.ForeignPtr.Unsafe(unsafeForeignPtrToPtr)
import Foreign.Marshal.Alloc
import Foreign.Ptr
import Foreign.C.String
import Foreign.Storable
import Data.ByteString.Internal as BS ( mallocByteString, ByteString(PS) )
import DuckDB.FFI
import qualified Data.ByteString as B
import Control.Exception
import Conduit
import Data.Aeson
import Data.Aeson.Lens
import qualified Data.Aeson.KeyMap as DAKM
import Data.Traversable
import Control.Lens
import qualified Data.Text as DT
import qualified Data.Aeson.Key as Key
import Data.Int
import Data.Word
import Control.Monad
import DuckDB.Types
import DuckDB.Utils

duckdbOpenInternal :: Maybe String -> IO (Ptr DuckDBDatabase)
duckdbOpenInternal mPath = do
  outDatabase <- malloc
  cPath <- maybe (pure nullPtr) newCString mPath
  result <- c_duckdb_open cPath outDatabase
  when (not (result == 0)) (throwIO $ userError "Failed to open DuckDB database.")
  pure outDatabase

duckdbOpenExtInternal :: Maybe String -> DuckDBConfig -> IO (Ptr DuckDBDatabase)
duckdbOpenExtInternal mPath config = do
  outDatabase <- malloc
  cPath <- maybe (pure nullPtr) newCString mPath
  result <- c_duckdb_open_ext cPath outDatabase config nullPtr
  when (not (result == 0)) (throwIO $ userError "Failed to open DuckDB database.")
  pure outDatabase

getConfigFromHM :: (Traversable t) => t (String, String) -> IO (Ptr DuckDBConfig)
getConfigFromHM items = do
  config <- duckdbCreateConfig
  configPtr <- peek config
  traverse (\(key,value) -> duckdbSetConfig configPtr key value) items
  pure config

duckdbConnectInternal :: Ptr CDuckDBDatabase -> IO (Ptr DuckDBConnection)
duckdbConnectInternal dataBase = do
  outConnection <- malloc
  result <- c_duckdb_connect dataBase outConnection
  when (not (result == 0)) (throwIO $ userError "Failed to Connect to DuckDB database.")
  pure outConnection

duckdbOpen :: (Traversable t) => Maybe String -> Maybe (t (String, String)) -> IO DuckDbCon
duckdbOpen mPath mConfigItems = do
  (ptr, config) <- case mConfigItems of
          Just items -> do 
            config <-  getConfigFromHM items
            configPtr <- peek config
            dbptr <- duckdbOpenExtInternal mPath configPtr
            pure (dbptr, Just config)
          Nothing -> do
            dbptr <- duckdbOpenInternal mPath
            pure (dbptr, Nothing)
  db <- peek ptr
  con <-  duckdbConnectInternal db
  pure $ DuckDbCon con ptr config

duckdbQuery :: DuckDbCon -> String -> IO ()
duckdbQuery DuckDbCon{connection} query = do
  alloca $ \resPtr -> do
    cquery <- newCString query
    con <- peek connection
    result <- c_duckdb_query con cquery resPtr
    when (not (result == 0)) (do
        err <- c_duckdb_result_error resPtr
        errorString <- peekCString err
        c_duckdb_destroy_result resPtr
        throwIO $ userError errorString)
    c_duckdb_destroy_result resPtr

getRowData :: Ptr CDuckDBDataChunk -> [DuckDBType] -> Int -> [String] -> ConduitT () Object IO ()
getRowData chunk types numCols cNames = do
  let numRows = fromEnum $ c_duckdb_data_chunk_get_size chunk
  columnsData <- mapM (\idxCol -> do
    let 
      vector = c_duckdb_data_chunk_get_vector chunk (toEnum idxCol)
      colData = c_duckdb_vector_get_data vector
      validatyCol = c_duckdb_vector_get_validity vector
    pure (colData , validatyCol)
      ) [0..numCols-1]
  forM_ [0..numRows-1] (\idxRow -> do
    let
      obj = mempty :: Object
    finalObj <- foldM (\o col -> do
      val <- if (fromEnum (c_duckdb_validity_row_is_valid (snd (columnsData !! col)) (toEnum idxRow)) == 1 )
              then liftIO $ Just <$> getMappedValues col idxRow columnsData types
              else pure Nothing
      pure $ (o & at (Key.fromText $ DT.pack (cNames !! col)) ?~ (toJSON val))
        ) obj [0..numCols-1]
    yield finalObj)

makeResultConduit :: Ptr DuckDBResult -> ConduitT () Object IO ()
makeResultConduit resultPtr = do
  let
    numCols = fromEnum $ c_duckdb_column_count resultPtr
    types = map (\idx -> toEnum (fromEnum $ c_duckdb_column_type resultPtr (toEnum idx)) :: DuckDBType) [0..numCols-1]
    loopFetch cNames= do
      chunkPtr <- liftIO $ c_duckdb_stream_fetch_chunk_ptr resultPtr
      when (not (chunkPtr == nullPtr)) (do
          getRowData chunkPtr types numCols cNames
          liftIO $ c_duckdb_destroy_data_chunk chunkPtr
          loopFetch cNames
        )
  colNames <- liftIO $ mapM (\idx -> peekCString $ c_duckdb_column_name resultPtr (toEnum idx)) [0..numCols-1]
  loopFetch colNames

duckdbQueryWithResponse :: DuckDbCon -> String -> ConduitT () Object IO ()
duckdbQueryWithResponse DuckDbCon{connection} query = do
  resPtr <- liftIO $ malloc
  psPtr <- liftIO $ malloc
  cquery <- liftIO $ newCString query
  con <- liftIO $ peek connection
  liftIO $ c_duckdb_prepare con cquery psPtr
  ps <- liftIO $ peek psPtr
  result <- liftIO $
              c_duckdb_execute_prepared_streaming ps resPtr
  when (not (result == 0)) (do
        err <- liftIO $ c_duckdb_result_error resPtr
        errorString <- liftIO $
                        case err == nullPtr of
                          False -> peekCString err
                          _ -> pure "Failed"
        liftIO $ throwIO $ userError errorString
        )
  makeResultConduit resPtr
  liftIO $ c_duckdb_destroy_result resPtr

duckdbRowCount :: Ptr DuckDBResult -> Int
duckdbRowCount resultPtr = fromEnum $ c_duckdb_row_count resultPtr

duckdbClose :: DuckDbCon ->  IO ()
duckdbClose DuckDbCon{connection, database, config} = do
  maybe (pure ()) duckdbDestroyConfig config
  c_duckdb_disconnect connection
  c_duckdb_close database

duckdbConfigureAWS :: DuckDbCon ->  IO ()
duckdbConfigureAWS res = do
  duckdbQuery res "INSTALL httpfs;"
  duckdbQuery res "LOAD httpfs;"
  duckdbQuery res "INSTALL aws;"
  duckdbQuery res "LOAD aws;"
  duckdbQuery res "CALL load_aws_credentials();"

duckdbCreateConfig :: IO (Ptr DuckDBConfig)
duckdbCreateConfig = do
  configPtr <- malloc
  result <- c_duckdb_create_config configPtr
  when (not (result == 0)) (throwIO $ userError "Failed to create config.")
  pure configPtr

duckdbSetConfig :: DuckDBConfig -> String -> String -> IO ()
duckdbSetConfig configPtr a b =  do
    key <- newCString a
    value <- newCString b
    result <- c_duckdb_set_config configPtr key value 
    when (not (result == 0)) (throwIO $ userError "Failed to set config.")

duckdbDestroyConfig :: Ptr DuckDBConfig -> IO ()
duckdbDestroyConfig  = c_duckdb_destroy_config
