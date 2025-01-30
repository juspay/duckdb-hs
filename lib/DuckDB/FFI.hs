{-# LANGUAGE MagicHash #-}

module DuckDB.FFI 

where

import Foreign
import Foreign.C.Types
import Foreign.C.String
import GHC.IO.Exception
import GHC.Ptr ( Ptr(Ptr) )
import GHC.CString (unpackCString#)

data LResult
data LDuckDBDatabase
data LDuckDBConnection
data LDuckDBConfig
data LDuckDBDataChunk
data LDuckDBVector
data LDuckDBValidity

--  Define types for duckdb_state, duckdb_database, and duckdb_config
type DuckDBState = CInt
type DuckDBDatabase = Ptr LDuckDBDatabase
type DuckDBConnection = Ptr LDuckDBConnection
type DuckDBResult = Ptr LResult
type DuckDBPreparedStatement = Ptr ()
type DuckDBConfig = Ptr LDuckDBConfig

-- Foreign imports for the DuckDB API
foreign import ccall "duckdb.h duckdb_open"
  c_duckdb_open :: CString -> Ptr DuckDBDatabase -> IO DuckDBState

foreign import ccall "duckdb.h duckdb_open_ext"
  c_duckdb_open_ext :: CString -> Ptr DuckDBDatabase -> DuckDBConfig -> Ptr (Ptr CChar) -> IO DuckDBState

foreign import ccall "duckdb.h duckdb_connect"
  c_duckdb_connect :: DuckDBDatabase -> Ptr DuckDBConnection -> IO DuckDBState

-- Query

foreign import ccall "duckdb.h duckdb_query"
  c_duckdb_query :: DuckDBConnection -> CString -> Ptr DuckDBResult -> IO DuckDBState

foreign import ccall "duckdb.h duckdb_fetch_chunk"
  c_duckdb_fetch_chunk :: DuckDBResult -> IO (Ptr LDuckDBDataChunk)

foreign import ccall "duckdb.h duckdb_stream_fetch_chunk"
  c_duckdb_stream_fetch_chunk :: DuckDBResult -> IO (Ptr LDuckDBDataChunk)

foreign import ccall "duckdb.h duckdb_stream_fetch_chunk_ptr"
  c_duckdb_stream_fetch_chunk_ptr :: Ptr DuckDBResult -> IO (Ptr LDuckDBDataChunk)
  

foreign import ccall "duckdb.h duckdb_data_chunk_get_size"
  c_duckdb_data_chunk_get_size :: Ptr LDuckDBDataChunk -> CInt

foreign import ccall "duckdb.h duckdb_data_chunk_get_vector"
  c_duckdb_data_chunk_get_vector :: Ptr LDuckDBDataChunk -> CInt -> Ptr LDuckDBVector

foreign import ccall "duckdb.h duckdb_vector_get_data"
  c_duckdb_vector_get_data :: Ptr LDuckDBVector -> Ptr ()

foreign import ccall "duckdb.h duckdb_vector_get_validity"
  c_duckdb_vector_get_validity :: Ptr LDuckDBVector -> CIntPtr

foreign import ccall "duckdb.h duckdb_cstring_from_struct_string"
  c_duckdb_cstring_from_struct_string :: Ptr () -> CInt -> CString

foreign import ccall "duck.h duckdb_get_bool_from_vector"
  c_duckdb_get_bool_from_vector :: Ptr () -> CInt -> CBool

foreign import ccall "duckdb.h duckdb_timestamp_from_struct"
  c_duckdb_timestamp_from_struct :: Ptr () -> CInt -> CLong

foreign import ccall "duckdb.h duckdb_validity_row_is_valid"
  c_duckdb_validity_row_is_valid :: CIntPtr -> CInt -> CBool

foreign import ccall "duckdb.h duckdb_row_count"
  c_duckdb_row_count :: Ptr DuckDBResult -> CInt

foreign import ccall "duckdb.h duckdb_destroy_data_chunk"
  c_duckdb_destroy_data_chunk :: (Ptr LDuckDBDataChunk) -> IO ()

foreign import ccall "duckdb.h duckdb_data_chunk_get_column_count"
  c_duckdb_data_chunk_get_column_count :: Ptr LDuckDBDataChunk -> CInt

foreign import ccall "duckdb.h duckdb_column_type"
  c_duckdb_column_type :: Ptr DuckDBResult -> CInt -> CInt

foreign import ccall "duckdb.h duckdb_column_name"
  c_duckdb_column_name :: Ptr DuckDBResult -> CInt -> CString
  
foreign import ccall "duckdb.h duckdb_column_count"
  c_duckdb_column_count :: Ptr DuckDBResult -> CInt

foreign import ccall "duckdb.h duckdb_result_error"
  c_duckdb_result_error :: Ptr DuckDBResult -> CString

foreign import ccall "duckdb.h duckdb_destroy_result"
  c_duckdb_destroy_result :: Ptr DuckDBResult -> IO ()

-- prepare query

foreign import ccall "duckdb.h duckdb_result_chunk_count"
  c_duckdb_result_chunk_count :: DuckDBResult -> CInt
  
foreign import ccall "duckdb.h duckdb_result_return_type"
  c_duckdb_result_return_type :: DuckDBResult -> CInt

foreign import ccall "duckdb.h duckdb_result_is_streaming"
  c_duckdb_result_is_streaming :: DuckDBResult -> CBool

foreign import ccall unsafe "duck.h duckdb_result_is_streaming_ptr"
  c_duckdb_result_is_streaming_ptr :: Ptr DuckDBResult -> CBool

foreign import ccall "duckdb.h duckdb_prepare"
  c_duckdb_prepare :: DuckDBConnection -> CString -> Ptr DuckDBPreparedStatement -> IO DuckDBState

foreign import ccall "duckdb.h duckdb_execute_prepared_streaming"
  c_duckdb_execute_prepared_streaming :: DuckDBPreparedStatement -> Ptr DuckDBResult -> IO DuckDBState

-- disconnect

foreign import ccall "duckdb.h duckdb_disconnect"
  c_duckdb_disconnect :: Ptr DuckDBConnection -> IO ()

foreign import ccall "duckdb.h duckdb_close"
  c_duckdb_close :: Ptr DuckDBDatabase -> IO ()

-- config

foreign import ccall "duckdb.h duckdb_create_config"
  c_duckdb_create_config :: Ptr DuckDBConfig -> IO DuckDBState

foreign import ccall "duckdb.h duckdb_set_config"
  c_duckdb_set_config :: DuckDBConfig -> CString -> CString -> IO DuckDBState

foreign import ccall "duckdb.h duckdb_destroy_config"
  c_duckdb_destroy_config :: Ptr DuckDBConfig -> IO ()
