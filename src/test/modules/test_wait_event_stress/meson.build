# Copyright (c) 2026, PostgreSQL Global Development Group

test_wait_event_stress_sources = files(
  'test_wait_event_stress.c',
)

if host_system == 'windows'
  test_wait_event_stress_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_wait_event_stress',
    '--FILEDESC', 'test_wait_event_stress - wait event timing overhead measurement',])
endif

test_wait_event_stress = shared_module('test_wait_event_stress',
  test_wait_event_stress_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_wait_event_stress

test_install_data += files(
  'test_wait_event_stress.control',
  'test_wait_event_stress--1.0.sql',
)

tests += {
  'name': 'test_wait_event_stress',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_wait_event_stress',
    ],
  },
  'tap': {
    'tests': [
      't/001_orphan_roundtrip.pl',
    ],
  },
}
