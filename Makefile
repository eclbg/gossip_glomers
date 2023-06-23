serve:
	maelstrom/maelstrom serve

# 1
echo:
	maelstrom/maelstrom test -w echo --bin target/debug/echo --node-count 1 --time-limit 10 

# 2
unique-ids:
	maelstrom/maelstrom test -w unique-ids --bin target/debug/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

# 3a
broadcast-single-node:
	maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast --node-count 1 --time-limit 20 --rate 10

# 3b
broadcast-multi-node:
	maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast --node-count 5 --time-limit 20 --rate 10

# 3c
broadcast-fault-tolerant:
	maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition

# 3d and 3e
broadcast-latency:
	maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast  --node-count 25 --time-limit 20 --rate 100 --latency 100

# 3d and 3e for checking correctness in presence of network partitions
broadcast-latency-partitioned:
	maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast  --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition

# 4
grow-only-counter:
	maelstrom/maelstrom test -w g-counter --bin target/debug/counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
	
# 5a
kafka-style-log-single-node:
	maelstrom/maelstrom test -w kafka --bin target/debug/single-node-kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
	
# 5b and 5c
kafka-style-log:
	maelstrom/maelstrom test -w kafka --bin target/debug/multi-node-kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

# 6a
transactions-single-node:
	maelstrom/maelstrom test -w txn-rw-register --bin target/debug/single-node-txn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

# 6b
transactions-read-uncommitted:
	maelstrom/maelstrom test -w txn-rw-register --bin target/debug/txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted

# 6b for checking correctness in presence of network partitions
transactions-read-uncommitted-totally-available:
	maelstrom/maelstrom test -w txn-rw-register --bin target/debug/txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --nemesis partition 

# 6c
transactions-read-committed-totally-available:
	maelstrom/maelstrom test -w txn-rw-register --bin target/debug/txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition

