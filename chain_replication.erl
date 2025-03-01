%%%-------------------------------------------------------------------
%%% @doc
%%% An implementation of Chain Replication in Erlang.
%%% This implementation includes a chain of replicas with:
%%% - A head node which accepts all writes
%%% - A tail node which serves all reads
%%% - Intermediate nodes which propagate updates and facilitate recovery
%%% @end
%%%-------------------------------------------------------------------

-module(chain_replication).
-export([start_node/2, stop_node/1, create_chain/1]).
-export([write/3, read/2, get_chain_status/1]).
-export([node_loop/1]).

-define(PROPAGATION_TIMEOUT, 5000). % Timeout for update propagation in ms

%%====================================================================
%% Types
%%====================================================================

-type node_id() :: term().
-type key() :: term().
-type value() :: term().
-type position() :: head | tail | {intermediate, non_neg_integer()}.
-type request_id() :: reference().
-type chain() :: [node_id()].

-record(state, {
    id :: node_id(),                       % This node's identifier
    position :: position(),                % Position in the chain (head, intermediate, or tail)
    next_id = none :: node_id() | none,    % ID of next node in the chain
    next_pid = none :: pid() | none,       % PID of next node in the chain
    prev_id = none :: node_id() | none,    % ID of previous node in the chain
    prev_pid = none :: pid() | none,       % PID of previous node in the chain
    chain = [] :: chain(),                 % All nodes in the chain in order
    data = #{} :: #{key() => value()},     % Key-value store for this node
    pending = #{} :: #{request_id() => {pid(), term()}}, % Pending requests
    version = 0 :: non_neg_integer()       % Current version number for data
}).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start a chain replication node with a given ID and position
-spec start_node(node_id(), position()) -> pid().
start_node(Id, Position) ->
    spawn(fun() -> 
        State = #state{id = Id, position = Position},
        node_loop(State)
    end).

%% @doc Stop a chain replication node
-spec stop_node(pid()) -> ok.
stop_node(Pid) ->
    Pid ! stop,
    ok.

%% @doc Create a chain from a list of node IDs
-spec create_chain([node_id()]) -> [{node_id(), pid()}].
create_chain(NodeIds) when length(NodeIds) >= 2 ->
    % Start all nodes
    Nodes = [
        begin
            Position = 
                if I =:= 1 -> head;
                   I =:= length(NodeIds) -> tail;
                   true -> {intermediate, I - 1}
                end,
            {Id, start_node(Id, Position)}
        end
        || {I, Id} <- lists:zip(lists:seq(1, length(NodeIds)), NodeIds)
    ],
    
    % Now configure the chain links
    configure_chain(Nodes, NodeIds),
    
    Nodes.

%% @doc Write a key-value pair to the chain (must be sent to head)
-spec write(pid(), key(), value()) -> {ok, version} | {error, term()}.
write(HeadNode, Key, Value) ->
    ReqId = make_ref(),
    HeadNode ! {write_request, self(), ReqId, Key, Value},
    receive
        {write_response, ReqId, Result} -> Result
    after ?PROPAGATION_TIMEOUT * 3 ->
        {error, timeout}
    end.

%% @doc Read a key's value from the chain (must be sent to tail)
-spec read(pid(), key()) -> {ok, value()} | {error, not_found} | {error, term()}.
read(TailNode, Key) ->
    ReqId = make_ref(),
    TailNode ! {read_request, self(), ReqId, Key},
    receive
        {read_response, ReqId, Result} -> Result
    after ?PROPAGATION_TIMEOUT ->
        {error, timeout}
    end.

%% @doc Get the current status of a node in the chain
-spec get_chain_status(pid()) -> {ok, map()}.
get_chain_status(Node) ->
    ReqId = make_ref(),
    Node ! {status_request, self(), ReqId},
    receive
        {status_response, ReqId, Status} -> {ok, Status}
    after ?PROPAGATION_TIMEOUT ->
        {error, timeout}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% Configure chain links by sending messages to all nodes
configure_chain(Nodes, NodeIds) ->
    Chain = NodeIds,
    
    % Create a mapping from node IDs to PIDs
    NodeMap = maps:from_list(Nodes),
    
    lists:foreach(
        fun({I, {Id, Pid}}) ->
            {PrevId, PrevPid} = 
                if I =:= 1 -> {none, none};
                   true -> 
                       PId = lists:nth(I - 1, NodeIds),
                       {PId, maps:get(PId, NodeMap)}
                end,
            {NextId, NextPid} = 
                if I =:= length(NodeIds) -> {none, none};
                   true -> 
                       NId = lists:nth(I + 1, NodeIds),
                       {NId, maps:get(NId, NodeMap)}
                end,
            Pid ! {configure, PrevId, PrevPid, NextId, NextPid, Chain}
        end,
        lists:zip(lists:seq(1, length(Nodes)), Nodes)
    ).

%% Main loop for chain replication nodes
node_loop(State) ->
    receive
        % Configuration message 
        {configure, PrevId, PrevPid, NextId, NextPid, Chain} ->
            NewState = State#state{
                prev_id = PrevId,
                prev_pid = PrevPid,
                next_id = NextId,
                next_pid = NextPid,
                chain = Chain
            },
            node_loop(NewState);
            
        % Write request - only processed by the head node
        {write_request, From, ReqId, Key, Value} ->
            case State#state.position of
                head ->
                    % Process the write
                    NewVersion = State#state.version + 1,
                    NewData = maps:put(Key, Value, State#state.data),
                    
                    % Store the client request to respond later
                    NewPending = maps:put(ReqId, {From, {write, Key, Value, NewVersion}}, 
                                         State#state.pending),
                    
                    % Propagate down the chain
                    case State#state.next_pid of
                        none ->
                            % Single node chain, auto-acknowledge
                            From ! {write_response, ReqId, {ok, NewVersion}},
                            node_loop(State#state{
                                data = NewData,
                                version = NewVersion,
                                pending = maps:remove(ReqId, NewPending)
                            });
                        NextPid ->
                            NextPid ! {update, ReqId, Key, Value, NewVersion},
                            node_loop(State#state{
                                data = NewData,
                                version = NewVersion,
                                pending = NewPending
                            })
                    end;
                _ ->
                    % Only head node should receive write requests
                    From ! {write_response, ReqId, {error, not_head}},
                    node_loop(State)
            end;
            
        % Read request - only processed by the tail node
        {read_request, From, ReqId, Key} ->
            case State#state.position of
                tail ->
                    % Process the read
                    Result = case maps:find(Key, State#state.data) of
                        {ok, Value} -> {ok, Value};
                        error -> {error, not_found}
                    end,
                    From ! {read_response, ReqId, Result},
                    node_loop(State);
                _ ->
                    % Only tail node should receive read requests
                    From ! {read_response, ReqId, {error, not_tail}},
                    node_loop(State)
            end;
            
        % Update propagating down the chain
        {update, ReqId, Key, Value, Version} ->
            % Apply the update to our state
            NewData = maps:put(Key, Value, State#state.data),
            
            case State#state.position of
                tail ->
                    % We're the tail, acknowledge back up the chain
                    case State#state.prev_pid of
                        none -> ok; % Should never happen
                        PrevPid -> PrevPid ! {ack, ReqId}
                    end,
                    node_loop(State#state{
                        data = NewData,
                        version = Version
                    });
                _ ->
                    % Propagate down the chain
                    case State#state.next_pid of
                        none -> ok; % Should never happen
                        NextPid -> NextPid ! {update, ReqId, Key, Value, Version}
                    end,
                    node_loop(State#state{
                        data = NewData,
                        version = Version
                    })
            end;
            
        % Acknowledgment propagating up the chain
        {ack, ReqId} ->
            case maps:find(ReqId, State#state.pending) of
                {ok, {From, {write, _, _, Version}}} ->
                    % This was a client request - respond to client
                    From ! {write_response, ReqId, {ok, Version}},
                    node_loop(State#state{
                        pending = maps:remove(ReqId, State#state.pending)
                    });
                error ->
                    % Propagate ack up the chain
                    case State#state.prev_pid of
                        none -> ok; % Head node with no matching pending request
                        PrevPid -> PrevPid ! {ack, ReqId}
                    end,
                    node_loop(State)
            end;
            
        % Status request
        {status_request, From, ReqId} ->
            Status = #{
                id => State#state.id,
                position => State#state.position,
                next => State#state.next_id,
                prev => State#state.prev_id,
                chain => State#state.chain,
                data_count => maps:size(State#state.data),
                version => State#state.version,
                pending_count => maps:size(State#state.pending)
            },
            From ! {status_response, ReqId, Status},
            node_loop(State);
            
        % Chain reconfiguration (node failure handling)
        {reconfigure, NewChain} ->
            % This is a simplified version of reconfiguration - a real implementation
            % would need more sophisticated handling of in-flight updates
            I = case lists:member(State#state.id, NewChain) of
                true -> get_position_in_list(State#state.id, NewChain);
                false -> 
                    % We're not in the new chain, terminate
                    exit(removed_from_chain)
            end,
            
            Position = 
                if I =:= 1 -> head;
                   I =:= length(NewChain) -> tail;
                   true -> {intermediate, I - 1}
                end,
                
            PrevId = 
                if I =:= 1 -> none;
                   true -> lists:nth(I - 1, NewChain)
                end,
                
            NextId = 
                if I =:= length(NewChain) -> none;
                   true -> lists:nth(I + 1, NewChain)
                end,
                
            % In a real implementation, we would need to resolve PIDs as well
            % This is simplified for the example
            NewState = State#state{
                position = Position,
                prev_id = PrevId,
                next_id = NextId,
                chain = NewChain
                % prev_pid and next_pid would need to be resolved
            },
            
            node_loop(NewState);
            
        % Data sync request (used during recovery)
        {sync_request, From, ReqId} ->
            From ! {sync_response, ReqId, State#state.data, State#state.version},
            node_loop(State);
            
        % Node join handling (for dynamic chains)
        {node_joined, NodeId, Position} ->
            % Handle node joining chain
            % Implementation would depend on specific requirements
            node_loop(State);
            
        % Node failure handling
        {node_failed, NodeId} ->
            % Handle node failure in chain
            % Implementation would depend on specific requirements
            node_loop(State);
            
        % Stop the node
        stop ->
            ok;
            
        % Unknown message
        Msg ->
            io:format("Node ~p received unknown message: ~p~n", [State#state.id, Msg]),
            node_loop(State)
    end.

% Helper to get position in a list
get_position_in_list(Item, List) ->
    get_position_in_list(Item, List, 1).
    
get_position_in_list(_, [], _) ->
    0;
get_position_in_list(Item, [Item | _], Pos) ->
    Pos;
get_position_in_list(Item, [_ | Rest], Pos) ->
    get_position_in_list(Item, Rest, Pos + 1).

%%====================================================================
%% Fault tolerance and recovery operations
%%====================================================================

%% Chain repair after node failure (simplified version)
handle_node_failure(FailedNodeId, Chain) ->
    % Get position of failed node
    Pos = get_position_in_list(FailedNodeId, Chain),
    if Pos =:= 0 ->
           Chain; % Node not in chain
       true ->
           % Remove the failed node
           NewChain = lists:delete(FailedNodeId, Chain),
           
           % Notify all remaining nodes of the new chain
           [Node ! {reconfigure, NewChain} || Node <- NewChain],
           
           % In a real implementation, we would also need to:
           % 1. Have the new nodes sync data
           % 2. Handle in-flight requests properly
           
           NewChain
    end.

%% Recovery mechanism to synchronize data (would be called as part of reconfiguration)
sync_node_data(NodeId, PredecessorId) ->
    % This is a simplified sync procedure
    ReqId = make_ref(),
    PredecessorId ! {sync_request, self(), ReqId},
    receive
        {sync_response, ReqId, Data, Version} ->
            NodeId ! {sync_data, Data, Version},
            {ok, synced}
    after ?PROPAGATION_TIMEOUT ->
        {error, sync_timeout}
    end.

%%====================================================================
%% Example usage
%%====================================================================

%% In the shell, you can test this with:
%%
%% 1> c(chain_replication).
%% 2> Nodes = chain_replication:create_chain([node1, node2, node3]).
%% 3> {_, HeadPid} = lists:nth(1, Nodes).
%% 4> {_, TailPid} = lists:nth(3, Nodes).
%%
%% Write to the head:
%% 5> chain_replication:write(HeadPid, key1, "value1").
%%
%% Read from the tail:
%% 6> chain_replication:read(TailPid, key1).
%%
%% Check status:
%% 7> chain_replication:get_chain_status(HeadPid).
%% 8> chain_replication:get_chain_status(TailPid).