#include "duckdb.h"

#ifdef __cplusplus
extern "C" {
#endif
  DUCKDB_API bool duckdb_result_is_streaming_ptr(duckdb_result *result);
  DUCKDB_API duckdb_data_chunk duckdb_stream_fetch_chunk_ptr(duckdb_result *result);
  DUCKDB_API const char *duckdb_cstring_from_struct_string(void *vectorData, idx_t row);
  DUCKDB_API int64_t duckdb_timestamp_from_struct(void *vectorData, idx_t row);
  DUCKDB_API bool duckdb_get_bool_from_vector(void *vectorData, idx_t row);
#ifdef __cplusplus
}
#endif