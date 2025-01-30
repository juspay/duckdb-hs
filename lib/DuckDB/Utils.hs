{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module DuckDB.Utils
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
import Data.ByteString.Internal as BS
    ( mallocByteString, ByteString(PS) )
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

getMappedValues :: Int -> Int -> [(Ptr (), b)] -> [DuckDBType] -> IO Value
getMappedValues col idxRow columnsData types = do
  case (types !! col) of
    Boolean -> pure $ toJSON $ fromEnum (c_duckdb_get_bool_from_vector (fst (columnsData !! col)) (toEnum idxRow)) == 1
    --  peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Bool) (idxRow)
    TinyInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Int8) (idxRow)
    SmallInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Int16) (idxRow)
    DuckInteger -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Int32) idxRow
    BigInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Int64) (idxRow)
    UTinyInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Word8) (idxRow)
    USmallInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Word16) (idxRow)
    UInteger -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Word32) (idxRow)
    UBigInt -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Word64) (idxRow)
    Float -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Float) (idxRow)
    DuckDouble -> toJSON <$> peekElemOff (castPtr (fst (columnsData !! col)) :: Ptr Double) (idxRow)
    Varchar -> toJSON <$> peekCString (c_duckdb_cstring_from_struct_string (fst (columnsData !! col)) (toEnum idxRow))
    Timestamp -> pure $ toJSON $ fromEnum (c_duckdb_timestamp_from_struct (fst (columnsData !! col)) (toEnum idxRow))
    _ -> pure $ toJSON Null
