#include "duckdb.h"
#include "duck.h"
#include <cstdio>
#include <cstdlib>

bool duckdb_result_is_streaming_ptr(duckdb_result *result) {
	if (!result) {
		return false;
	}
	duckdb_result res = *result;
	return duckdb_result_is_streaming(res);
}

duckdb_data_chunk duckdb_stream_fetch_chunk_ptr(duckdb_result *result) {
	if (!result) {
		return nullptr;
	}
	
	return duckdb_stream_fetch_chunk(*result);
}

const char *duckdb_cstring_from_struct_string(void *vectorData, idx_t row) {
    duckdb_string_t *vector_data = (duckdb_string_t *) vectorData;
    duckdb_string_t str = vector_data[row];
    if (duckdb_string_is_inlined(str)) {
        char *s = (char *)malloc(str.value.inlined.length+1);
        sprintf(s,"%.*s", str.value.inlined.length, str.value.inlined.inlined);
        return s;
    } else {
        char *s = (char *)malloc(str.value.pointer.length+1);
        sprintf(s,"%.*s", str.value.pointer.length, str.value.pointer.ptr);
        return s;
    }
    return "NULL";
}

bool duckdb_get_bool_from_vector(void *vectorData, idx_t row) {
    bool *vector_data = (bool *) vectorData;
    return vector_data[row];
}

int64_t duckdb_timestamp_from_struct(void *vectorData, idx_t row) {
    duckdb_timestamp *vector_data = (duckdb_timestamp *) vectorData;
    duckdb_timestamp t = vector_data[row];
    return t.micros;
}

