/*-------------------------------------------------------------------------
 *
 * pg_wait_event_tracing_data.h
 *    Dense wait-event map for the experiment's pinned PostgreSQL base.
 *
 * Generated from wait_event_names.txt at 74c77052bc by the v6
 * generate-wait_event_types.pl implementation.  Keeping the snapshot in
 * the module avoids changing PostgreSQL's ordinary generated headers.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_WAIT_EVENT_TRACING_DATA_H
#define PG_WAIT_EVENT_TRACING_DATA_H

#define PWET_RAW_CLASSES	12
#define PWET_DENSE_CLASSES	9
#define PWET_NUM_EVENTS		544

static const int8 pwet_class_dense[PWET_RAW_CLASSES] = {
	-1,							/* 0x00: unused */
	-1,							/* 0x01: LWLock (uses hash) */
	-1,							/* 0x02: unused */
	0,							/* 0x03: Lock */
	1,							/* 0x04: Buffer */
	2,							/* 0x05: Activity */
	3,							/* 0x06: Client */
	4,							/* 0x07: Extension */
	5,							/* 0x08: IPC */
	6,							/* 0x09: Timeout */
	7,							/* 0x0a: IO */
	8							/* 0x0b: InjectionPoint */
};

static const int pwet_class_nevents[PWET_DENSE_CLASSES] = {
	16,							/* Lock */
	16,							/* Buffer */
	32,							/* Activity */
	16,							/* Client */
	128,						/* Extension */
	64,							/* IPC */
	16,							/* Timeout */
	128,						/* IO */
	128							/* InjectionPoint */
};

static const int pwet_class_offset[PWET_DENSE_CLASSES] = {
	0,							/* Lock */
	16,							/* Buffer */
	32,							/* Activity */
	64,							/* Client */
	80,							/* Extension */
	208,						/* IPC */
	272,						/* Timeout */
	288,						/* IO */
	416							/* InjectionPoint */
};

static const uint8 pwet_dense_to_classid[PWET_DENSE_CLASSES] = {
	0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b
};

#endif							/* PG_WAIT_EVENT_TRACING_DATA_H */
