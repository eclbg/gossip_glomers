serve:
	maelstrom/maelstrom serve

echo:
	maelstrom/maelstrom test -w echo --bin from_scratch/target/debug/echo --node-count 1 --time-limit 10 

unique-ids:
	maelstrom/maelstrom test -w unique-ids --bin from_scratch/target/debug/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

broadcast-single-node:
	maelstrom/maelstrom test -w broadcast --bin from_scratch/target/debug/single-node-broadcast --node-count 1 --time-limit 20 --rate 10

broadcast-multi-node:
	maelstrom/maelstrom test -w broadcast --bin from_scratch/target/debug/multi-node-broadcast --node-count 5 --time-limit 20 --rate 10

broadcast-fault-tolerant:
	maelstrom/maelstrom test -w broadcast --bin from_scratch/target/debug/fault-tolerant-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition

broadcast-latency:
	maelstrom/maelstrom test -w broadcast --bin from_scratch/target/debug/fault-tolerant-broadcast  --node-count 25 --time-limit 20 --rate 100 --latency 100

broadcast-latency-partitioned:
	maelstrom/maelstrom test -w broadcast --bin from_scratch/target/debug/fault-tolerant-broadcast  --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition

grow-only-counter:
	maelstrom/maelstrom test -w g-counter --bin with_lib/target/debug/g-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
	
kafka-style-log-single-node:
	maelstrom/maelstrom test -w kafka --bin with_lib/target/debug/single-node-kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
	
kafka-style-log:
	maelstrom/maelstrom test -w kafka --bin with_lib/target/debug/multi-node-kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

# For testing
# kafka-style-log:
# 	RUST_LOG=debug maelstrom/maelstrom test -w kafka --bin with_lib/target/debug/multi-node-kafka --node-count 2 --concurrency 2n --time-limit 5 --rate 200

kafka-style-log-single-node-single-threaded:
	RUST_LOG=debug maelstrom/maelstrom test -w kafka --bin from_scratch/target/debug/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

transactions-single-node:
	maelstrom/maelstrom test -w txn-rw-register --bin with_lib/bin/txn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
