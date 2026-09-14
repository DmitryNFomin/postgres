# src/test/modules/test_wait_primitive/Makefile

MODULE_big = test_wait_primitive
OBJS = \
	$(WIN32RES) \
	test_wait_primitive.o
PGFILEDESC = "test_wait_primitive - wait primitive microbenchmarks"

EXTENSION = test_wait_primitive
DATA = test_wait_primitive--1.0.sql

REGRESS = test_wait_primitive

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_wait_primitive
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
