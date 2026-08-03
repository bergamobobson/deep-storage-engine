import os
import json


class LogEntry:
    """
    A single entry in the Raft log.

    Equivalent to a WAL record, but simpler — Raft's durability comes
    from replication across a majority of nodes, not from a single
    disk's CRC/fsync guarantees alone.

    Attributes:
        index   : position of this entry in the log (1, 2, 3, ...)
        term    : the term during which this entry was created by the leader
        command : the actual operation to apply to the state machine,
                  e.g. {"op": "put", "key": "banana", "value": "999"}
    """

    def __init__(self, index: int, term: int, command: dict):
        self.index   = index
        self.term    = term
        self.command = command


class NodeState:
    """
    Persistent state of a single Raft node — the subset of state that
    MUST survive a crash and be reloaded from disk on restart.

    Per the Raft paper, exactly three fields are persistent :
        - current_term
        - voted_for
        - log

    Everything else (commit_index, last_applied, next_index, match_index)
    is volatile and lives in Node, not here — it is safe to recompute
    after a restart or a new election.

    Why these three specifically must be durable :
        - current_term : without it, a restarted node could accept a
          RequestVote from a term it already knows is stale, or vote
          twice in the same term after "forgetting" its previous vote.
        - voted_for     : without it, a restarted node could grant a
          second vote in a term it already voted in — breaking Raft's
          "one vote per term" safety guarantee, which can lead to two
          leaders being elected in the same term (split brain).
        - log           : without it, a node that acknowledged an entry
          to the leader could lose that entry on restart, making the
          leader wrongly believe the entry is safely replicated.
    """

    def __init__(self, node_id: str):
        self.node_id      = node_id
        self.current_term = 0        # monotonically increasing election counter
        self.voted_for    = None     # candidate_id this node voted for in current_term
        self.log          = []       # list[LogEntry], append-only

    def save(self, path: str) -> None:
        """
        Persist current_term, voted_for, and log to disk as JSON.

        Must be called BEFORE responding "yes" to any RPC that changes
        this state (granting a vote, appending an entry) — otherwise a
        crash between the in-memory update and the disk write would let
        this node "forget" a promise it already made to another node.

        flush() + fsync() ensures the write survives a power loss, not
        just a process crash — same durability contract as the WAL.
        """
        payload = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [
                {"index": entry.index, "term": entry.term, "command": entry.command}
                for entry in self.log
            ]
        }
        with open(path, "w") as f:
            json.dump(payload, f)
            f.flush()                    # Python buffer → OS
            os.fsync(f.fileno())         # OS → disk

    def load(self, path: str) -> None:
        """
        Reload current_term, voted_for, and log from disk.

        Called once at node startup, before the node starts participating
        in elections or accepting AppendEntries — a node must know its
        last known term and vote before it can safely respond to RPCs.
        """
        with open(path, "r") as f:
            payload = json.load(f)
            self.current_term = payload["current_term"]
            self.voted_for = payload["voted_for"]
            self.log = [
                LogEntry(entry["index"], entry["term"], entry["command"])
                for entry in payload["log"]
            ]


if __name__ == "__main__":
    # Example usage — write state, simulate a crash, reload from disk
    state = NodeState("node1")
    state.current_term = 1
    state.voted_for = "node2"
    state.log.append(LogEntry(1, 1, {"action": "set", "key": "x", "value": 42}))
    state.save("state.json")

    # simulate restart — fresh object, nothing in memory
    new_state = NodeState("node1")
    new_state.load("state.json")
    print(new_state.current_term)   # should print 1
    print(new_state.voted_for)      # should print "node2"
    print(new_state.log[0].command) # should print {"action": "set", "key": "x", "value": 42}