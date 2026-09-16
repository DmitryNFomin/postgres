#!/usr/bin/env python3
"""Run turbostat and prefix every emitted line with an epoch timestamp."""

import argparse
import signal
import subprocess
import sys
import time
from pathlib import Path


PROCESS = None
STOP = False


def request_stop(_signum, _frame):
    global STOP
    STOP = True
    if PROCESS is not None and PROCESS.poll() is None:
        PROCESS.terminate()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("errors", type=Path)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    global PROCESS
    with args.errors.open("x", encoding="utf-8") as errors:
        PROCESS = subprocess.Popen(
            [
                "turbostat",
                "--quiet",
                "--Summary",
                "--interval",
                "1",
            ],
            stdout=subprocess.PIPE,
            stderr=errors,
            universal_newlines=True,
            bufsize=1,
        )
        assert PROCESS.stdout is not None
        with args.output.open("x", encoding="utf-8") as output:
            for line in PROCESS.stdout:
                timestamp_ns = int(time.time() * 1_000_000_000)
                output.write("{}\t{}".format(timestamp_ns, line))
                output.flush()
                if STOP:
                    break
        if PROCESS.poll() is None:
            PROCESS.terminate()
        try:
            return_code = PROCESS.wait(timeout=10)
        except subprocess.TimeoutExpired:
            PROCESS.kill()
            return_code = PROCESS.wait()
    if STOP and return_code in (0, -signal.SIGTERM):
        return 0
    if return_code != 0:
        print(
            "turbostat exited with status {}".format(return_code),
            file=sys.stderr,
        )
        return return_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
