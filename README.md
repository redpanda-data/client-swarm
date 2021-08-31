
# Quickstart

This tool is for  stressing Redpanda with lots of concurrent
producer connections.

    # Get a rust toolchain
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

    # 2000 producers sending 100 messages each
    # (Set your BROKERS and TOPIC as needed)
    cargo run --release -- producers $BROKERS $TOPIC 2000 100


