from enum import Enum
from raft.state import NodeState


class Role(str, Enum):
    """The three roles a Raft node can be in at any given time."""
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"


class Node:
    """
    A single server in a Raft cluster.

    Holds both the persistent state (NodeState — current_term, voted_for,
    log) and the volatile state that does NOT need to survive a crash
    (role, next_index, match_index) — these are safe to recompute after
    a restart or a new election.
    """

    def __init__(self, node_id: str, peers: list, state_path: str):
        """
        Args:
            node_id    : unique identifier for this node in the cluster
            peers      : list of the OTHER node ids in the cluster
                         (every node has this, regardless of role)
            state_path : file path where this node's NodeState is
                         persisted to disk (current_term, voted_for, log)
        """
        self.node_id    = node_id
        self.peers      = peers
        self.role       = Role.FOLLOWER   # every node starts as follower
        self.timeout    = 0                # randomized election timeout
        self.state_path = state_path
        self.state      = NodeState(node_id)
        self.commit_index = 0   # ← ajoute cette ligne

        # --- leader-only volatile state ---
        # next_index  : per peer, the next log entry the leader THINKS
        #               it should send. Optimistic estimate, decremented
        #               on rejection.
        self.next_index = {}

        # match_index : per peer, the highest log entry CONFIRMED to be
        #               replicated. Used to compute what is committed —
        #               an entry is committed once a majority's
        #               match_index reaches it.
        self.match_index = {}

    def is_log_up_to_date(self, last_log_term: int, last_log_index: int) -> bool:
        """
        Compare a candidate's log recency against this node's own log.

        Raft's election safety rule: a candidate can only be granted a
        vote if its log is at least as up-to-date as the voter's log.
        This guarantees any newly elected leader already has every
        previously committed entry.

        Comparison rule: term is compared first (more recent term always
        wins), and only if terms are equal does log length (index) decide.

        Returns:
            True if the candidate's log is at least as up to date as mine.
        """
        my_last_term  = self.state.log[-1].term  if self.state.log else 0
        my_last_index = self.state.log[-1].index if self.state.log else 0

        if last_log_term != my_last_term:
            return last_log_term > my_last_term
        return last_log_index >= my_last_index

    def grant_vote(self, candidate_id: str, candidate_term: int,
                   last_log_term: int, last_log_index: int) -> bool:
        """
        Handle an incoming RequestVote RPC and decide whether to grant
        this node's vote to the candidate.

        Three conditions must ALL hold for the vote to be granted :
            1. candidate_term >= my current_term
               (a stale candidate from an old term is always rejected)
            2. I haven't already voted for someone else this term
               (one vote per term — prevents two leaders in the same term)
            3. the candidate's log is at least as up to date as mine
               (election safety — see is_log_up_to_date)

        If granted, current_term and voted_for are updated AND persisted
        to disk BEFORE returning True. This ordering matters: if the node
        crashed after returning True but before persisting, it could
        "forget" its own vote on restart and grant a second, conflicting
        vote in the same term — breaking Raft's safety guarantee.

        Returns:
            True if the vote is granted, False otherwise.
        """
        # reject a candidate from a stale (older) term
        if candidate_term < self.state.current_term:
            return False

        # already voted for a different candidate this term
        if self.state.voted_for not in (None, candidate_id):
            return False

        # candidate must not be behind on committed entries
        if not self.is_log_up_to_date(last_log_term, last_log_index):
            return False

        # grant the vote — persist BEFORE returning True
        self.state.voted_for    = candidate_id
        self.state.current_term = candidate_term
        self.state.save(self.state_path)

        return True

    def check_log_consistency(self, prev_log_index: int, prev_log_term: int) -> bool:
        """
        Verify the follower's log agrees with the leader's log up to
        prev_log_index, before accepting any new entries.

        This is Raft's log matching property in action: if the entry at
        prev_log_index has the same term on both leader and follower, then
        every entry before it is guaranteed identical too. Checking a single
        position is enough to confirm the entire prefix matches.

        Three cases:
            1. prev_log_index == 0 → no previous entry to check, this is
            the very first entry ever sent. Always consistent.
            2. prev_log_index > len(self.state.log) → I don't even have an
            entry at that position yet. Reject — I'm missing entries.
            3. my entry at prev_log_index has a different term → my log
            diverged from the leader's at some point in the past
            (e.g. a different, now-stale leader wrote something there).
            Reject — the leader must back up and resend from an earlier point.

        Returns:
            True if it is safe to append the leader's new entries after
            prev_log_index, False otherwise.
        """
        if prev_log_index == 0:
            return True

        if prev_log_index > len(self.state.log):
            return False

        if self.state.log[prev_log_index - 1].term != prev_log_term:
            return False

        return True

    def append_entries(self, leader_id: str, leader_term: int,
                    prev_log_index: int, prev_log_term: int,
                    entries: list, leader_commit: int) -> bool:
        """
        Handle an incoming AppendEntries RPC from the leader.

        Used both for actual log replication (entries non-empty) and for
        heartbeats (entries empty) — a heartbeat is just an AppendEntries
        with nothing new to add, sent periodically so followers know the
        leader is still alive and don't start an election.

        Steps, in order:
            1. reject stale leader — if leader_term < my current_term,
            this message is from an outdated leader, ignore it.
            2. accept the leader — update current_term if the leader's
            term is newer, and step down to FOLLOWER (a candidate or
            even a leader must yield once it sees a higher term).
            3. check log consistency at prev_log_index / prev_log_term —
            if it doesn't match, reject so the leader backs up and
            retries with an earlier prev_log_index.
            4. append the new entries — any conflicting entry already
            present at the same index is overwritten (the leader's
            version always wins once consistency is confirmed).
            5. advance commit_index — I can safely consider an entry
            committed once the leader tells me it is, but never beyond
            what I actually have in my own log.
            6. persist state to disk before returning True — same
            durability rule as grant_vote: never acknowledge something
            that isn't safely on disk yet.

        Returns:
            True if the entries were accepted, False otherwise.
        """
        # 1. reject a stale leader
        if leader_term < self.state.current_term:
            return False

        # 2. a real leader exists at this term (or later) — step down
        if leader_term >= self.state.current_term:
            self.state.current_term = leader_term
            self.role = Role.FOLLOWER

        # 3. verify the log agrees with the leader up to prev_log_index
        if not self.check_log_consistency(prev_log_index, prev_log_term):
            return False

        # 4. append new entries, overwriting any conflicting ones
        #    prev_log_index is 1-based; entries start right after it
        insert_at = prev_log_index  # 0-based position in self.state.log
        for offset, entry in enumerate(entries):
            position = insert_at + offset
            if position < len(self.state.log):
                # overwrite if there's a conflict, otherwise leave as-is
                if self.state.log[position].term != entry.term:
                    self.state.log = self.state.log[:position]
                    self.state.log.append(entry)
            else:
                self.state.log.append(entry)

        # 5. advance commit_index, never past what I actually have
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.state.log))

        # 6. persist before acknowledging
        self.state.save(self.state_path)

        return True

if __name__ == "__main__":
    n1 = Node("node1", peers=["node2", "node3"], state_path="node1_state.json")

    granted = n1.grant_vote("node2", candidate_term=1, last_log_term=0, last_log_index=0)
    print(granted)                    # → True
    print(n1.state.current_term)      # → 1
    print(n1.state.voted_for)         # → node2

    granted2 = n1.grant_vote("node3", candidate_term=1, last_log_term=0, last_log_index=0)
    print(granted2)                   # → False

    n1 = Node("node1", peers=[], state_path="node1_state.json")

    from raft.state import LogEntry
    n1.state.log = [
        LogEntry(1, 1, {"op": "put", "key": "a", "value": "1"}),
        LogEntry(2, 1, {"op": "put", "key": "b", "value": "2"}),
        LogEntry(3, 2, {"op": "put", "key": "c", "value": "3"}),
    ]

    print(n1.check_log_consistency(0, 0))   # → True  (no previous entry)
    print(n1.check_log_consistency(3, 2))   # → True  (entry 3 has term 2, matches)
    print(n1.check_log_consistency(3, 1))   # → False (entry 3 has term 2, not 1)
    print(n1.check_log_consistency(5, 2))   # → False (I don't have entry 5)


    n1 = Node("node1", peers=[], state_path="node1_state.json")

    from raft.state import LogEntry

    ok = n1.append_entries(
        leader_id="node2",
        leader_term=1,
        prev_log_index=0,
        prev_log_term=0,
        entries=[LogEntry(1, 1, {"op": "put", "key": "a", "value": "1"})],
        leader_commit=1
    )
    print(ok)                          # → True
    print(len(n1.state.log))           # → 1
    print(n1.commit_index)             # → 1
    print(n1.state.current_term)       # → 1