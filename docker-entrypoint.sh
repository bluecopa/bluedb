#!/bin/sh
# Run bluedb-server under libfaketime so the Jepsen clock-skew nemesis can shift
# this node's WALL clock at runtime (Docker containers share the VM kernel clock,
# so `date -s` can't skew one node — libfaketime is the per-process seam).
#
# - Offset is read from a file the nemesis rewrites (`echo '-8s' > /faketime/offset`).
# - DONT_FAKE_MONOTONIC keeps CLOCK_MONOTONIC real, so tokio timers / network
#   timeouts are unaffected — only SystemTime::now() (the lease math) skews.
# - CACHE_DURATION=1 makes an offset change take effect within ~1s.
#
# With no libfaketime present (or an empty offset) the server runs normally.
set -e

LIB="$(ls /usr/lib/*/faketime/libfaketime.so.1 2>/dev/null | head -n1)"
mkdir -p /faketime
[ -f /faketime/offset ] || echo '+0' > /faketime/offset

if [ -n "$LIB" ]; then
  export LD_PRELOAD="$LIB"
  export FAKETIME_TIMESTAMP_FILE=/faketime/offset
  export FAKETIME_CACHE_DURATION=1
  export FAKETIME_DONT_FAKE_MONOTONIC=1
fi

exec bluedb-server
