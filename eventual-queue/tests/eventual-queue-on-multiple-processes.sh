#!/bin/zsh

trap terminate SIGINT
terminate(){
    pkill -SIGINT -P $$
    exit
}

num_of_peers=8

for (( i=0; i < $num_of_peers; ++i ))
do
    (RUST_LOG=trace cargo run --package eventual-queue --bin eventual-queue -- "$i" "$num_of_peers") &
done

wait