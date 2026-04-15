MODULES = test_wait_event_stress
PGFILEDESC = "test_wait_event_stress - wait event timing overhead measurement"

EXTENSION = test_wait_event_stress
DATA = test_wait_event_stress--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_wait_event_stress
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
