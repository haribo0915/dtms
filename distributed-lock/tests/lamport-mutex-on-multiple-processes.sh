#!/bin/zsh

trap terminate SIGINT
terminate(){
    pkill -SIGINT -P $$
    exit
}

num_of_peers=10

for (( i=0; i < $num_of_peers; ++i ))
do
    (RUST_LOG=trace cargo run --package lamport-mutex --bin lamport-mutex -- "$i" "$num_of_peers") &
done

wait