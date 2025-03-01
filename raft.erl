% 1> c(raft).
% {ok,raft}
% 2> raft:start(server1, [server2, server3]).
% {ok,<0.92.0>}
% 3> raft:start(server2, [server1, server3]).
% {ok,<0.94.0>}
% 4> raft:start(server3, [server1, server2]).
% {ok,<0.96.0>}
% 5> raft:get_state(server1).
% {follower,0,0}
% 6> raft:get_state(server2).
% {follower,0,0}
% 7> raft:get_state(server3).
% {follower,0,0}
% 8> raft:become_leader(server1).
% ok
% 9> raft:get_state(server1).
% {leader,1,0}
% 10> raft:get_state(server3).
% {follower,1,0}
% 11> raft:get_state(server1).
% {leader,1,0}
% 12> simple_raft:add_entry(server1, {set, key1, "value1"}).
% ** exception error: undefined function simple_raft:add_entry/2
% 13> raft:add_entry(server1, {set, key1, "value1"}).
% Applying entry: {set,key1,"value1"}
% {ok,1}
% Applying entry: {set,key1,"value1"}
% Applying entry: {set,key1,"value1"}
% 14> raft:add_entry(server1, {set, key2, "value2"}).
% Applying entry: {set,key2,"value2"}
% {ok,2}
% Applying entry: {set,key2,"value2"}
% Applying entry: {set,key2,"value2"}
% 15> raft:get_state(server1).
% {leader,1,2}
% 16> raft:get_state(server2).
% {follower,1,2}
% 17> raft:get_state(server3).
% {follower,1,2}
% 18> raft:become_leader(server3).
% ok
% 19> raft:get_state(server1).
% {follower,2,2}
% 20> raft:get_state(server2).
% {follower,2,2}
% 21> raft:get_state(server3).
% {leader,2,2}
% 22>


-module(raft).

%% API
-export([start/2, stop/1, add_entry/2, get_state/1, become_leader/1]).

%% Internal exports
-export([server_loop/1]).

%% Server State record
-record(state, {
    id,                     % Server ID
    peers = [],            % List of other server PIDs
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

%% Public API

% Start a Raft server
start(ServerId, PeerIds) ->
    % Register the server by its ID
    ServerPid = spawn(?MODULE, server_loop, [#state{
        id = ServerId,
        peers = PeerIds
    }]),
    register(ServerId, ServerPid),
    {ok, ServerPid}.

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

%% Internal functions

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
            
        % Timer events (leader heartbeats)
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
            
        % Any other message
        _Other ->
            % Ignore unknown messages
            server_loop(State)
    end.

% Helper function to get peer PIDs from their registered names
get_peer_pids(State) ->
    [whereis(Peer) || Peer <- State#state.peers, whereis(Peer) /= undefined].

% Get the term of the last log entry, or 0 if log is empty
get_last_log_term(State) ->
    case State#state.log of
        [] -> 0;
        Log -> element(1, lists:last(Log))
    end.

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
    PossibleIndices = [I || I <- lists:seq(CurrentCommitIndex + 1, length(State#state.log))],
    
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