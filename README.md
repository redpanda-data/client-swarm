
# client-swarm

## Quickstart

This tool is for stressing Redpanda with lots of concurrent
producer connections.

### Build and run

```
# Get a rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2000 producers sending 100 messages each
# (Set your BROKERS and TOPIC as needed)
cargo run --release -- --brokers localhost:9092 producers --topic $TOPIC --count 2000 --messages 100
```

### Using the wrapper

Using very high client counts (several 1000s of connections) will generally hit some operating system limits and fail to create all the desired clients. To avoid this you can run the swarm using the wrapper script, which sets these limits to much higher values. Some of these modifications apply system-wide and will persist until the next reboot.


```
./wrapper.sh --brokers localhost:9092 producers --topic $TOPIC --count 2000 --messages 100
```
