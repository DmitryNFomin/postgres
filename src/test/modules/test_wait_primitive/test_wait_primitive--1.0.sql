/* src/test/modules/test_wait_primitive/test_wait_primitive--1.0.sql */

\echo Use "CREATE EXTENSION test_wait_primitive" to load this file. \quit

CREATE FUNCTION test_wait_primitive_latch_set(bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'test_wait_primitive_latch_set'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

CREATE FUNCTION test_wait_primitive_latch_timeout(bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'test_wait_primitive_latch_timeout'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

CREATE FUNCTION test_wait_primitive_file_read(bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'test_wait_primitive_file_read'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

CREATE FUNCTION test_wait_primitive_usleep0(bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'test_wait_primitive_usleep0'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

CREATE FUNCTION test_wait_primitive_report_only(bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'test_wait_primitive_report_only'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION test_wait_primitive_latch_set(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION test_wait_primitive_latch_timeout(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION test_wait_primitive_file_read(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION test_wait_primitive_usleep0(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION test_wait_primitive_report_only(bigint) FROM PUBLIC;
