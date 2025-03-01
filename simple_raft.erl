%% simple_raft.erl
-module(simple_raft).

%% API
-export([start/2, stop/1, add_entry/2, get_state/1, become_leader/1]).
-export([start_with_discovery/1, discover_peers/0]).
-export([start_with_auto_election/1, start_election/1]).

%% Internal exports
-export([server_loop/1]).

%% Server State record
-record(state, {
    id,                     % Server ID
    peers = [],            % List of other server IDs
    current_term = 0,      % Latest term server has seen
    voted_for = undefined, % CandidateId that received vote in current term
    log = [],              % Log entries: [{Term, Entry}]
    commit_index = 0,      % Index of highest log entry known to be committed
    last_applied = 0,      % Index of highest log entry applied to state machine
    role = follower,       % Current role: follower, candidate, or leader
    leader_id = undefined, % Current leader (if known)
    votes_received = [],   % Votes received in current election
    next_index = #{},      % For each server, index of next log entry to send
    match_index = #{}      % For each server, index of highest log known to be replicated
}).

%% ===================================================================
%% Public API
%% ===================================================================

% Start a Raft server
start(ServerId, PeerIds) ->
    % Register the server by its ID
    ServerPid = spawn(?MODULE, server_loop, [#state{
        id = ServerId,
        peers = PeerIds
    }]),
    register(ServerId, ServerPid),
    {ok, ServerPid}.

% Start a server with automatic peer discovery
start_with_discovery(ServerId) ->
    % Discover peers on the network
    Peers = discover_peers(),
    % Filter out self
    OtherPeers = lists:delete(ServerId, Peers),
    % Start the server with discovered peers
    start(ServerId, OtherPeers).

% Start with discovery and automatic election
start_with_auto_election(ServerId) ->
    % Discover peers
    Peers = discover_peers(),
    OtherPeers = lists:delete(ServerId, Peers),
    
    % Start the server
    {ok, Pid} = start(ServerId, OtherPeers),
    
    % Schedule a random election timeout
    schedule_random_first_election(ServerId),
    
    {ok, Pid}.

% Stop a Raft server
stop(ServerId) ->
    ServerPid = whereis(ServerId),
    ServerPid ! {stop, self()},
    receive
        {stopped, ServerPid} -> ok
    after 5000 ->
        exit(ServerPid, kill),
        ok
    end.

% Add an entry to the log (only works if sent to the leader)
add_entry(ServerId, Entry) ->
    ServerPid = whereis(ServerId),
    ServerPid ! {add_entry, Entry, self()},
    receive
        {entry_result, Result} -> Result
    after 5000 ->
        {error, timeout}
    end.

% Get the current state of a server
get_state(ServerId) ->
    ServerPid = whereis(ServerId),
    ServerPid ! {get_state, self()},
    receive
        {state_info, Info} -> Info
    after 5000 ->
        {error, timeout}
    end.

% Manually make a server the leader
become_leader(ServerId) ->
    ServerPid = whereis(ServerId),
    ServerPid ! {become_leader, self()},
    receive
        {become_leader_result, Result} -> Result
    after 5000 ->
        {error, timeout}
    end.

% Start an election for this server
start_election(ServerId) ->
    ServerPid = whereis(ServerId),
    ServerPid ! start_election,
    ok.

%% ===================================================================
%% Peer Discovery
%% ===================================================================

% Simple node discovery using Erlang's built-in functionality
discover_peers() ->
    % If we're in a distributed Erlang setup
    PeerIds = case is_alive() of
        true ->
            % 1. Try to connect to nodes in the same cluster
            {ok, Names} = net_adm:names(),
            PeerNodes = [list_to_atom(Name ++ "@" ++ inet_gethostname()) || 
                        {Name, _} <- Names],
            
            % 2. Connect to all discovered nodes
            [net_kernel:connect_node(Node) || Node <- PeerNodes],
            
            % 3. Get registered Raft servers from all connected nodes
            RegisteredNames = lists:flatten(
                [rpc:call(Node, erlang, registered, []) || 
                 Node <- [node() | nodes()]]
            ),
            
            % 4. Filter to only get IDs that match raft_server naming pattern
            [Id || Id <- RegisteredNames, is_raft_server(Id)];
        false ->
            % In non-distributed mode, just check locally
            [Id || Id <- registered(), is_raft_server(Id)]
    end,
    
    % Return discovered peers
    PeerIds.

% Helper to check if a registered name is a Raft server
is_raft_server(Id) ->
    % Simple check - modify based on your naming convention
    % For example, if all your Raft servers have names starting with 'server'
    case atom_to_list(Id) of
        "server" ++ _ -> true;
        _ -> false
    end.

% Get the hostname for node naming
inet_gethostname() ->
    {ok, Hostname} = inet:gethostname(),
    Hostname.

% Schedule the first election attempt with random delay
schedule_random_first_election(ServerId) ->
    % Wait a bit for all servers to start (200-500ms)
    InitialDelay = 200 + rand:uniform(300),
    erlang:send_after(InitialDelay, whereis(ServerId), election_timeout),
    ok.

%% ===================================================================
%% Server Implementation
%% ===================================================================

% The main server loop
server_loop(State) ->
    receive
        % Client API messages
        {add_entry, Entry, From} ->
            case State#state.role of
                leader ->
                    NewLog = State#state.log ++ [{State#state.current_term, Entry}],
                    NewState = State#state{log = NewLog},
                    % Replicate to peers
                    [Peer ! {append_entries, self(), State#state.current_term, 
                             length(State#state.log), get_last_log_term(State), 
                             [{State#state.current_term, Entry}], 
                             State#state.commit_index} 
                     || Peer <- get_peer_pids(State)],
                    From ! {entry_result, {ok, length(NewLog)}},
                    server_loop(NewState);
                _ ->
                    From ! {entry_result, {error, {not_leader, State#state.role, State#state.leader_id}}},
                    server_loop(State)
            end;
            
        {get_state, From} ->
            From ! {state_info, {State#state.role, State#state.current_term, length(State#state.log)}},
            server_loop(State);
            
        {become_leader, From} ->
            NewState = do_become_leader(State),
            From ! {become_leader_result, ok},
            server_loop(NewState);
            
        {stop, From} ->
            From ! {stopped, self()},
            exit(normal);
            
        % Raft protocol messages
        {request_vote, CandidateId, Term, LastLogIndex, LastLogTerm} ->
            case State#state.role of
                leader ->
                    % Leaders never grant votes
                    CandidateId ! {vote_response, self(), State#state.current_term, false},
                    server_loop(State);
                _ ->
                    {NewState, VoteGranted} = handle_vote_request(CandidateId, Term, LastLogIndex, LastLogTerm, State),
                    CandidateId ! {vote_response, self(), NewState#state.current_term, VoteGranted},
                    server_loop(NewState)
            end;
            
        {vote_response, From, Term, Granted} ->
            NewState = handle_vote_response(From, Term, Granted, State),
            server_loop(NewState);
            
        {append_entries, LeaderId, Term, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit} ->
            case State#state.role of
                leader ->
                    % Leaders never accept append entries
                    LeaderId ! {append_response, self(), State#state.current_term, false},
                    server_loop(State);
                _ ->
                    {NewState, Success} = handle_append_entries(LeaderId, Term, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit, State),
                    LeaderId ! {append_response, self(), NewState#state.current_term, Success},
                    server_loop(NewState)
            end;
            
        {append_response, From, Term, Success} ->
            NewState = handle_append_response(From, Term, Success, State),
            server_loop(NewState);
            
        % Timer events
        send_heartbeats ->
            case State#state.role of
                leader ->
                    % Send AppendEntries to all peers
                    [Peer ! {append_entries, self(), State#state.current_term, 
                             length(State#state.log), get_last_log_term(State), 
                             [], State#state.commit_index} 
                     || Peer <- get_peer_pids(State)],
                    % Reschedule heartbeat
                    schedule_heartbeat(),
                    server_loop(State);
                _ ->
                    server_loop(State)
            end;
            
        % Automatic election trigger
        start_election ->
            NewState = begin_election(State),
            server_loop(NewState);
        
        % Election timeout
        election_timeout ->
            case State#state.role of
                leader -> 
                    % Leaders don't respond to election timeouts
                    server_loop(State);
                _ ->
                    % Start a new election if we're not a leader
                    NewState = begin_election(State),
                    server_loop(NewState)
            end;
            
        % Any other message
        _Other ->
            % Ignore unknown messages
            server_loop(State)
    end.

%% ===================================================================
%% Helper Functions
%% ===================================================================

% Helper function to get peer PIDs from their registered names
get_peer_pids(State) ->
    [whereis(Peer) || Peer <- State#state.peers, whereis(Peer) /= undefined].

% Get the term of the last log entry, or 0 if log is empty
get_last_log_term(State) ->
    case State#state.log of
        [] -> 0;
        Log -> element(1, lists:last(Log))
    end.

% Begin an election
begin_election(State) ->
    % Increment term
    NewTerm = State#state.current_term + 1,
    % Vote for self
    NewState = State#state{
        role = candidate,
        current_term = NewTerm,
        voted_for = self(),
        votes_received = [self()]
    },
    
    % Request votes from all peers
    LastLogIndex = length(State#state.log),
    LastLogTerm = get_last_log_term(State),
    
    [Peer ! {request_vote, self(), NewTerm, LastLogIndex, LastLogTerm}
     || Peer <- get_peer_pids(NewState)],
    
    % Set election timeout
    schedule_election_timeout(),
    
    NewState.

% Handle a vote request
handle_vote_request(CandidateId, Term, LastLogIndex, LastLogTerm, State) ->
    if
        % If term > currentTerm, convert to follower
        Term > State#state.current_term ->
            NewState = become_follower(State, Term),
            handle_vote_request(CandidateId, Term, LastLogIndex, LastLogTerm, NewState);
            
        % Reply false if term < currentTerm
        Term < State#state.current_term ->
            {State, false};
            
        % If votedFor is undefined or candidateId, and candidate's log is at least as up-to-date as local log, grant vote
        true ->
            VotedFor = State#state.voted_for,
            LogOk = is_candidate_log_up_to_date(LastLogIndex, LastLogTerm, State),
            
            if
                (VotedFor == undefined orelse VotedFor == CandidateId) andalso LogOk ->
                    % Grant vote
                    NewState = State#state{voted_for = CandidateId},
                    % Reset election timeout since we voted
                    schedule_election_timeout(),
                    {NewState, true};
                true ->
                    {State, false}
            end
    end.

% Check if candidate's log is at least as up-to-date as ours
is_candidate_log_up_to_date(LastLogIndex, LastLogTerm, State) ->
    MyLastLogIndex = length(State#state.log),
    MyLastLogTerm = get_last_log_term(State),
    
    if
        LastLogTerm > MyLastLogTerm -> true;
        LastLogTerm < MyLastLogTerm -> false;
        true -> LastLogIndex >= MyLastLogIndex
    end.

% Handle a vote response
handle_vote_response(From, Term, Granted, State) ->
    if
        % If not a candidate, ignore
        State#state.role /= candidate ->
            State;
            
        % If term > currentTerm, convert to follower
        Term > State#state.current_term ->
            become_follower(State, Term);
            
        % If term < currentTerm, ignore
        Term < State#state.current_term ->
            State;
            
        % If granted, add to votes
        Granted ->
            VotesReceived = [From | State#state.votes_received],
            NewState = State#state{votes_received = VotesReceived},
            
            % Check if we've won the election
            QuorumSize = (length(State#state.peers) + 1) div 2 + 1,
            if
                length(VotesReceived) >= QuorumSize ->
                    do_become_leader(NewState);
                true ->
                    NewState
            end;
            
        % If not granted, do nothing
        true ->
            State
    end.

% Handle an AppendEntries RPC
handle_append_entries(LeaderId, Term, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit, State) ->
    if
        % If term < currentTerm, reject
        Term < State#state.current_term ->
            {State, false};
            
        % If term >= currentTerm
        true ->
            % Update term and become follower if needed
            NewState = if
                Term > State#state.current_term ->
                    become_follower(State, Term);
                true ->
                    % Always become follower when receiving valid AppendEntries
                    % (unless already a follower)
                    case State#state.role of
                        follower -> State#state{leader_id = LeaderId};
                        _ -> State#state{role = follower, leader_id = LeaderId}
                    end
            end,
            
            % Reset election timeout since we heard from a valid leader
            schedule_election_timeout(),
            
            % Check if log contains an entry at PrevLogIndex with PrevLogTerm
            LogOk = check_previous_log_entry(PrevLogIndex, PrevLogTerm, NewState#state.log),
            
            if
                not LogOk ->
                    {NewState, false};
                true ->
                    % Process entries
                    NewLog = process_entries(PrevLogIndex, NewState#state.log, Entries),
                    NewState2 = NewState#state{log = NewLog},
                    
                    % Update commit index
                    NewState3 = update_commit_index(NewState2, LeaderCommit),
                    
                    {NewState3, true}
            end
    end.

% Check if log has the entry at PrevLogIndex with PrevLogTerm
check_previous_log_entry(0, _PrevLogTerm, _Log) ->
    % Special case for empty log
    true;
check_previous_log_entry(PrevLogIndex, PrevLogTerm, Log) ->
    if
        PrevLogIndex > length(Log) ->
            false;
        true ->
            {Term, _} = lists:nth(PrevLogIndex, Log),
            Term == PrevLogTerm
    end.

% Process entries (removing conflicting entries and appending new ones)
process_entries(PrevLogIndex, Log, Entries) ->
    % Remove all entries after PrevLogIndex
    LogPrefix = lists:sublist(Log, PrevLogIndex),
    LogPrefix ++ Entries.

% Update commit index based on leader commit
update_commit_index(State, LeaderCommit) ->
    if
        LeaderCommit > State#state.commit_index ->
            NewCommitIndex = min(LeaderCommit, length(State#state.log)),
            apply_committed_entries(State, NewCommitIndex);
        true ->
            State
    end.

% Apply newly committed entries to state machine
apply_committed_entries(State, NewCommitIndex) ->
    LastApplied = State#state.last_applied,
    if
        NewCommitIndex > LastApplied ->
            EntriesToApply = lists:sublist(State#state.log, LastApplied + 1, NewCommitIndex - LastApplied),
            lists:foreach(fun({_Term, Entry}) ->
                % In a real implementation, you would apply these to your state machine
                io:format("Applying entry: ~p~n", [Entry])
            end, EntriesToApply),
            State#state{commit_index = NewCommitIndex, last_applied = NewCommitIndex};
        true ->
            State#state{commit_index = NewCommitIndex}
    end.

% Handle AppendEntries response
handle_append_response(From, Term, Success, State) ->
    if
        % If not a leader, ignore
        State#state.role /= leader ->
            State;
            
        % If term > currentTerm, step down
        Term > State#state.current_term ->
            become_follower(State, Term);
            
        % If term < currentTerm, ignore
        Term < State#state.current_term ->
            State;
            
        % Process response
        Success ->
            % Update match_index and next_index for this peer
            % Note: in a complete implementation, you would track which entries were sent to each peer
            NextIndex = maps:get(From, State#state.next_index, 1),
            MatchIndex = maps:get(From, State#state.match_index, 0),
            
            % Simple update - assume all entries were received
            NewNextIndex = length(State#state.log) + 1,
            NewMatchIndex = length(State#state.log),
            
            NewState = State#state{
                next_index = maps:put(From, NewNextIndex, State#state.next_index),
                match_index = maps:put(From, NewMatchIndex, State#state.match_index)
            },
            
            % Check if we can advance commit_index
            update_leader_commit_index(NewState);
            
        % If not successful, retry with a previous entry
        true ->
            NextIndex = maps:get(From, State#state.next_index, 1),
            NewNextIndex = max(1, NextIndex - 1),
            NewState = State#state{
                next_index = maps:put(From, NewNextIndex, State#state.next_index)
            },
            
            % Retry with earlier log entry
            PrevLogIndex = NewNextIndex - 1,
            PrevLogTerm = if
                PrevLogIndex == 0 -> 0;
                true ->
                    {Term, _} = lists:nth(PrevLogIndex, State#state.log),
                    Term
            end,
            
            % Get entries to send
            Entries = if
                NewNextIndex > length(State#state.log) ->
                    [];
                true ->
                    lists:nthtail(NewNextIndex - 1, State#state.log)
            end,
            
            From ! {append_entries, self(), State#state.current_term, 
                   PrevLogIndex, PrevLogTerm, Entries, 
                   State#state.commit_index},
            
            NewState
    end.

% Update leader's commit index based on replicated logs
update_leader_commit_index(State) ->
    % Find indices that have been replicated to a majority of servers
    CurrentCommitIndex = State#state.commit_index,
    
    % Only consider indices in current term
    PossibleIndices = [I || I <- lists:seq(CurrentCommitIndex + 1, length(State#state.log)),
                          I > 0, I =< length(State#state.log)],
    
    % Find highest index replicated to a majority
    NewCommitIndex = find_new_commit_index(PossibleIndices, State),
    
    if
        NewCommitIndex > CurrentCommitIndex ->
            apply_committed_entries(State, NewCommitIndex);
        true ->
            State
    end.

% Find highest index replicated to a majority with current term
find_new_commit_index([], State) ->
    State#state.commit_index;
find_new_commit_index([Index | Rest], State) ->
    % Check if the entry is from current term
    {EntryTerm, _} = lists:nth(Index, State#state.log),
    
    if
        EntryTerm /= State#state.current_term ->
            find_new_commit_index(Rest, State);
        true ->
            % Count servers with matchIndex >= Index
            ReplicatedCount = length([P || P <- maps:keys(State#state.match_index), 
                                     maps:get(P, State#state.match_index) >= Index]) + 1,
            
            % Check if we have a majority
            QuorumSize = (length(State#state.peers) + 1) div 2 + 1,
            if
                ReplicatedCount >= QuorumSize ->
                    Index;
                true ->
                    find_new_commit_index(Rest, State)
            end
    end.

% Transition to leader role
do_become_leader(State) ->
    % Initialize leader state
    NextIndex = maps:from_list([{whereis(P), length(State#state.log) + 1} || 
                               P <- State#state.peers, whereis(P) /= undefined]),
    MatchIndex = maps:from_list([{whereis(P), 0} || 
                                P <- State#state.peers, whereis(P) /= undefined]),
    
    % Increment term when becoming a leader
    NewTerm = State#state.current_term + 1,
    
    NewState = State#state{
        role = leader,
        leader_id = self(),
        current_term = NewTerm,
        next_index = NextIndex,
        match_index = MatchIndex
    },
    
    % Send initial empty AppendEntries RPCs (heartbeats)
    [Peer ! {append_entries, self(), NewState#state.current_term, 
            length(NewState#state.log), get_last_log_term(NewState), 
            [], NewState#state.commit_index} 
     || Peer <- get_peer_pids(NewState)],
    
    % Schedule regular heartbeats
    schedule_heartbeat(),
    
    NewState.

% Transition to follower role
become_follower(State, Term) ->
    State#state{
        role = follower,
        current_term = Term,
        voted_for = undefined
    }.

% Schedule a heartbeat message
schedule_heartbeat() ->
    erlang:send_after(50, self(), send_heartbeats).

% Schedule an election timeout
schedule_election_timeout() ->
    % Random timeout between 150-300ms
    Timeout = 150 + rand:uniform(150),
    erlang:send_after(Timeout, self(), election_timeout).