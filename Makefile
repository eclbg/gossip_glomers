serve:
	maelstrom/maelstrom serve

echo:
	maelstrom/maelstrom test -w echo --bin target/debug/echo --node-count 1 --time-limit 10 

unique-ids:
	maelstrom/maelstrom test -w unique-ids --bin target/debug/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

broadcast-single-node:
	maelstrom/maelstrom test -w broadcast --bin target/debug/single-node-broadcast --node-count 1 --time-limit 20 --rate 10

broadcast-multi-node:
	maelstrom/maelstrom test -w broadcast --bin target/debug/multi-node-broadcast --node-count 5 --time-limit 20 --rate 10

broadcast-fault-tolerant:
	maelstrom/maelstrom test -w broadcast --bin target/debug/fault-tolerant-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition

broadcast-latency:
	maelstrom/maelstrom test -w broadcast --bin target/debug/fault-tolerant-broadcast  --node-count 25 --time-limit 20 --rate 100 --latency 100

broadcast-latency-partitioned:
	maelstrom/maelstrom test -w broadcast --bin target/debug/fault-tolerant-broadcast  --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition
