#!/usr/bin/env bash

# wrapper script to set ulimits and friends such that high connection counts can be used
# Run as root: sudo ./wrapper.sh /path/to/binary --brokers foobar --topic foo ...

set -e

echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse
echo 15000 64000 > /proc/sys/net/ipv4/ip_local_port_range
echo 1000000 > /proc/sys/vm/max_map_count
ulimit -n 1000000
ulimit -s 200000
ulimit -i unlimited
ulimit -u unlimited

RUST_LOG=info,librdkafka=debug $@
