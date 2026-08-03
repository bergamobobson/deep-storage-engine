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

if __name__ == "__main__":
    n1 = Node("node1", peers=["node2", "node3"], state_path="node1_state.json")

    granted = n1.grant_vote("node2", candidate_term=1, last_log_term=0, last_log_index=0)
    print(granted)                    # → True
    print(n1.state.current_term)      # → 1
    print(n1.state.voted_for)         # → node2

    granted2 = n1.grant_vote("node3", candidate_term=1, last_log_term=0, last_log_index=0)
    print(granted2)                   # → False