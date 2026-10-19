# Jepsen tests for echo_pubsub

These tests check echo_pubsub's delivery promise under real faults: iptables
partitions, SIGKILL, and SIGSTOP pauses, on five Debian nodes.

- `node_app/` is a small Elixir release. Each node runs one copy. It runs
  `Phoenix.PubSub` with the `EchoPubSub` adapter, takes broadcasts over HTTP,
  and appends every message it receives to `/opt/echo/deliveries.log`.
- `echo/` is the Jepsen test written in Clojure. It installs the release on
  each node, broadcasts unique ids while injecting faults, heals the cluster,
  reads every node's log, and checks the results.

What these tests found and the results of each run are in [RESULTS.md](RESULTS.md).

## What is checked

A *stream* is one producer boot: a node plus the id of the VM that acked the
broadcast. A restart starts a new stream with an empty buffer.

| Verdict | Meaning | Fails the run? |
| --- | --- | --- |
| `:lost` | A subscriber never logged an acked broadcast, and nothing explains it. | yes |
| `:lost-to-crash` | The producer restarted later, and its in-memory buffer died with it. | no |
| `:lost-to-subscriber-crash` | The subscriber restarted after the broadcast was invoked, so its mailbox died with it. | no |
| `:lost-after-expiry` | The subscriber logged `cursor_expired` for the producer after the broadcast was invoked. | no |
| `:gaps` | A subscriber logged message m before a message x from the same stream that was acked before m was invoked. | yes, unless a `cursor_expired` or a subscriber restart was logged before m and after x was invoked |
| `:duplicates` | The same message was logged more than once. | no (at-least-once) |
| `:unexplained-expired` | `cursor_expired` from a producer that never restarted. | only in the `delivery` workload |

Each log line carries `ts`, the node's wall clock in milliseconds, and each
client op carries `:wall-invoke`. The Docker nodes share the host's clock, so
the checker only accepts a restart or expiry as an excuse when it happened
after the broadcast it excuses.

There are two workloads:

- `delivery` uses a buffer of 1,000,000 messages, so no node should fall far
  enough behind to expire.
- `expiry` uses a buffer of 50 messages. Partitioned nodes expire, and every
  gap they see must be explained by a `cursor_expired`.

## Running

1. Build the release tarball. Run this from the echo_pubsub repo root:

   ```sh
   docker build -f jepsen/node_app/Dockerfile.build -o jepsen/echo/resources .
   ```

2. Start Jepsen's 5-node Docker cluster from a checkout of jepsen-io/jepsen
   (tag `v0.3.7`):

   ```sh
   cd jepsen/docker && bin/up --daemon
   ```

3. Copy this test into the control node and run it:

   ```sh
   docker cp jepsen/echo jepsen-control:/root/echo
   docker exec -it jepsen-control bash -c \
     'cd /root/echo && lein run test --workload delivery --nemesis partition,kill,pause --time-limit 300 --test-count 5'
   ```

   Results are written to `store/latest/results.edn`. To browse every run, use
   `lein run serve` and forward port 8080 from the control node.

Useful options are `--rate`, `--nemesis-interval`, `--buffer-size`,
`--batch-interval`, `--call-timeout` and `--quiet-period`. Run
`lein run test --help` to see them all.

### Canary

Running the `delivery` workload with a tiny buffer must fail. If it does not,
the checker is not catching real violations:

```sh
lein run test --workload delivery --buffer-size 50 --nemesis partition --time-limit 120
```

## Tests

```sh
cd jepsen/node_app && mix test
cd jepsen/echo && lein test
```
