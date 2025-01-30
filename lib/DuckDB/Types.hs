{-# LANGUAGE MagicHash #-}
{-# LANGUAGE OverloadedStrings #-}

module DuckDB.Types 

where

import Foreign ( Ptr )
import DuckDB.FFI

data DuckDbCon = DuckDbCon {
  connection :: Ptr DuckDBConnection
  , database :: Ptr DuckDBDatabase
  , config :: Maybe (Ptr DuckDBConfig)
}

data DuckDBType
    = Invalid
    | Boolean
    | TinyInt
    | SmallInt
    | DuckInteger
    | BigInt
    | UTinyInt
    | USmallInt
    | UInteger
    | UBigInt
    | Float
    | DuckDouble
    | Timestamp
    | Date
    | Time
    | Interval
    | HugeInt
    | Varchar
    | Blob
    | Decimal
    | TimestampS
    | TimestampMS
    | TimestampNS
    | Enum
    | List
    | Struct
    | Map
    | UUID
    | Union
    | Bit
    | TimeTZ
    | TimestampTZ
    | UHugeInt
    | Array
    | Any
    | VarInt
    | SQLNull
    deriving (Show, Eq, Enum)