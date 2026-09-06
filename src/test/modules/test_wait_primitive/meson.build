# Copyright (c) 2026, PostgreSQL Global Development Group

test_wait_primitive_sources = files(
  'test_wait_primitive.c',
)

if host_system == 'windows'
  test_wait_primitive_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_wait_primitive',
    '--FILEDESC', 'test_wait_primitive - wait primitive microbenchmarks',])
endif

test_wait_primitive = shared_module('test_wait_primitive',
  test_wait_primitive_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_wait_primitive

test_install_data += files(
  'test_wait_primitive.control',
  'test_wait_primitive--1.0.sql',
)

tests += {
  'name': 'test_wait_primitive',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_wait_primitive',
    ],
  },
}
