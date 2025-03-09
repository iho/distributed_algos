-module(ekafka).
-export([
    start/1,
    stop/0,
    create_topic/2,
    delete_topic/1,
    list_topics/0,
    produce/3,
    consume/2,
    consume_batch/3,
    get_offsets/1,
    set_consumer_offset/3
]).

%% Required Raft callbacks
-export([
    init/1,
    apply/3,
    state_enter/2
]).

-record(server_state, {
    node_id,
    raft_peers,
    data_dir,
    topics = #{},         % Map of topic name -> topic config
    consumer_groups = #{} % Map of group ID -> consumer group state
}).

-record(topic, {
    name,
    partitions,
    replication_factor,
    partition_leaders = #{} % Map of partition ID -> leader node
}).

-record(consumer_group, {
    id,
    members = #{},        % Map of consumer ID -> subscribed topics
    offsets = #{}         % Map of {Topic, Partition} -> Offset
}).

-record(message, {
    key,
    value,
    timestamp,
    offset,
    headers = []
}).

%% ===================================================================
%% Public API
%% ===================================================================

%% Starts the Erlang Kafka service
start(Config) ->
    % Extract configuration
    DataDir = maps:get(data_dir, Config, "data"),
    NodeId = maps:get(node_id, Config, node()),
    RaftPeers = maps:get(raft_peers, Config, []),
    
    % Create data directory if it doesn't exist
    ok = filelib:ensure_dir(DataDir ++ "/"),
    
    % Initialize RocksDB
    {ok, _} = application:ensure_all_started(rocksdb),
    
    % Start Raft
    {ok, _} = application:ensure_all_started(ra),
    
    % Initialize state
    State = #server_state{
        node_id = NodeId,
        raft_peers = RaftPeers,
        data_dir = DataDir
    },
    
    % Register process
    register(ekafka_server, spawn(fun() -> server_loop(State) end)),
    
    % Initialize Raft cluster if this is the first node
    case RaftPeers of
        [] ->
            init_raft_cluster(NodeId, [NodeId]);
        _ ->
            init_raft_cluster(NodeId, [NodeId | RaftPeers])
    end,
    
    % Load existing topics
    LoadedTopics = load_topics_from_disk(DataDir),
    update_server_state(fun(S) -> S#server_state{topics = LoadedTopics} end),
    
    % Load consumer groups
    LoadedGroups = load_consumer_groups_from_disk(DataDir),
    update_server_state(fun(S) -> S#server_state{consumer_groups = LoadedGroups} end),
    
    {ok, ekafka_server}.

%% Stops the Erlang Kafka service
stop() ->
    case whereis(ekafka_server) of
        undefined -> 
            {error, not_running};
        Pid ->
            Pid ! stop,
            ok
    end.

%% Creates a new topic
create_topic(Name, Config) ->
    Partitions = maps:get(partitions, Config, 1),
    ReplicationFactor = maps:get(replication_factor, Config, 1),
    
    call_server({create_topic, Name, Partitions, ReplicationFactor}).

%% Deletes a topic
delete_topic(Name) ->
    call_server({delete_topic, Name}).

%% Lists all topics
list_topics() ->
    call_server(list_topics).

%% Produces a message to a topic/partition
produce(Topic, Partition, Message) ->
    call_server({produce, Topic, Partition, Message}).

%% Consumes a single message from a topic/partition
consume(Topic, Partition) ->
    call_server({consume, Topic, Partition}).

%% Consumes a batch of messages from a topic/partition
consume_batch(Topic, Partition, MaxMessages) ->
    call_server({consume_batch, Topic, Partition, MaxMessages}).

%% Gets the current offsets for a topic
get_offsets(Topic) ->
    call_server({get_offsets, Topic}).

%% Sets the consumer offset for a consumer group
set_consumer_offset(GroupId, Topic, Partition) ->
    call_server({set_consumer_offset, GroupId, Topic, Partition}).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% Main server loop
server_loop(State) ->
    receive
        {call, From, Request} ->
            {Reply, NewState} = handle_call(Request, State),
            From ! {ekafka_reply, Reply},
            server_loop(NewState);
        
        {cast, Request} ->
            NewState = handle_cast(Request, State),
            server_loop(NewState);
        
        stop ->
            % Clean shutdown
            shutdown(State);
        
        {raft_commit, Command} ->
            NewState = apply_raft_command(Command, State),
            server_loop(NewState);
        
        UnknownMessage ->
            io:format("Unknown message: ~p~n", [UnknownMessage]),
            server_loop(State)
    end.

%% Handle synchronous calls
handle_call({create_topic, Name, Partitions, ReplicationFactor}, State) ->
    % Check if topic already exists
    case maps:is_key(Name, State#server_state.topics) of
        true ->
            {{error, topic_already_exists}, State};
        false ->
            % Create topic record
            Topic = #topic{
                name = Name,
                partitions = Partitions,
                replication_factor = ReplicationFactor
            },
            
            % Propose via Raft
            Command = {create_topic, Topic},
            case propose_to_raft(Command) of
                {ok, Result} ->
                    % Create partition directories
                    TopicDir = filename:join(State#server_state.data_dir, Name),
                    ok = filelib:ensure_dir(TopicDir ++ "/"),
                    
                    % Create partition files
                    [create_partition(Name, P, State#server_state.data_dir) || P <- lists:seq(0, Partitions-1)],
                    
                    % Update topics map
                    NewTopics = maps:put(Name, Topic, State#server_state.topics),
                    NewState = State#server_state{topics = NewTopics},
                    
                    % Save to disk
                    save_topics_to_disk(NewTopics, State#server_state.data_dir),
                    
                    {Result, NewState};
                Error ->
                    {Error, State}
            end
    end;

handle_call({delete_topic, Name}, State) ->
    % Check if topic exists
    case maps:find(Name, State#server_state.topics) of
        {ok, Topic} ->
            % Propose via Raft
            Command = {delete_topic, Name},
            case propose_to_raft(Command) of
                {ok, Result} ->
                    % Delete partition files
                    TopicDir = filename:join(State#server_state.data_dir, Name),
                    [delete_partition(Name, P, State#server_state.data_dir) || P <- lists:seq(0, Topic#topic.partitions-1)],
                    
                    % Remove directory
                    file:del_dir(TopicDir),
                    
                    % Update topics map
                    NewTopics = maps:remove(Name, State#server_state.topics),
                    NewState = State#server_state{topics = NewTopics},
                    
                    % Save to disk
                    save_topics_to_disk(NewTopics, State#server_state.data_dir),
                    
                    {Result, NewState};
                Error ->
                    {Error, State}
            end;
        error ->
            {{error, topic_not_found}, State}
    end;

handle_call(list_topics, State) ->
    TopicNames = maps:keys(State#server_state.topics),
    {TopicNames, State};

handle_call({produce, Topic, Partition, MessageData}, State) ->
    % Check if topic exists
    case maps:find(Topic, State#server_state.topics) of
        {ok, TopicInfo} ->
            % Check if partition exists
            case Partition < TopicInfo#topic.partitions of
                true ->
                    % Forward to leader if needed
                    case maps:get(Partition, TopicInfo#topic.partition_leaders, State#server_state.node_id) of
                        NodeId when NodeId =:= State#server_state.node_id ->
                            % We are the leader for this partition
                            % Create message record
                            Timestamp = erlang:system_time(millisecond),
                            
                            % Get current offset
                            CurrentOffset = get_current_offset(Topic, Partition, State#server_state.data_dir),
                            
                            % Construct message
                            Message = case is_map(MessageData) of
                                true ->
                                    #message{
                                        key = maps:get(key, MessageData, undefined),
                                        value = maps:get(value, MessageData, <<>>),
                                        timestamp = Timestamp,
                                        offset = CurrentOffset,
                                        headers = maps:get(headers, MessageData, [])
                                    };
                                false ->
                                    % Assume binary data
                                    #message{
                                        key = undefined,
                                        value = MessageData,
                                        timestamp = Timestamp,
                                        offset = CurrentOffset
                                    }
                            end,
                            
                            % Write to RocksDB
                            Result = write_message_to_db(Topic, Partition, Message, State#server_state.data_dir),
                            
                            % Update offset
                            update_offset(Topic, Partition, CurrentOffset + 1, State#server_state.data_dir),
                            
                            {Result, State};
                        LeaderNode ->
                            % Forward to leader
                            {rpc:call(LeaderNode, ?MODULE, produce, [Topic, Partition, MessageData]), State}
                    end;
                false ->
                    {{error, invalid_partition}, State}
            end;
        error ->
            {{error, topic_not_found}, State}
    end;

handle_call({consume, Topic, Partition}, State) ->
    % Check if topic exists
    case maps:find(Topic, State#server_state.topics) of
        {ok, TopicInfo} ->
            % Check if partition exists
            case Partition < TopicInfo#topic.partitions of
                true ->
                    % Get next message
                    {read_message_from_db(Topic, Partition, 1, State#server_state.data_dir), State};
                false ->
                    {{error, invalid_partition}, State}
            end;
        error ->
            {{error, topic_not_found}, State}
    end;

handle_call({consume_batch, Topic, Partition, MaxMessages}, State) ->
    % Check if topic exists
    case maps:find(Topic, State#server_state.topics) of
        {ok, TopicInfo} ->
            % Check if partition exists
            case Partition < TopicInfo#topic.partitions of
                true ->
                    % Get batch of messages
                    {read_message_from_db(Topic, Partition, MaxMessages, State#server_state.data_dir), State};
                false ->
                    {{error, invalid_partition}, State}
            end;
        error ->
            {{error, topic_not_found}, State}
    end;

handle_call({get_offsets, Topic}, State) ->
    % Check if topic exists
    case maps:find(Topic, State#server_state.topics) of
        {ok, TopicInfo} ->
            % Collect offsets for all partitions
            OffsetMap = maps:from_list([
                {P, get_current_offset(Topic, P, State#server_state.data_dir)} 
                || P <- lists:seq(0, TopicInfo#topic.partitions-1)
            ]),
            {OffsetMap, State};
        error ->
            {{error, topic_not_found}, State}
    end;

handle_call({set_consumer_offset, GroupId, Topic, Partition}, State) ->
    % Check if topic exists
    case maps:find(Topic, State#server_state.topics) of
        {ok, TopicInfo} ->
            % Check if partition exists
            case Partition < TopicInfo#topic.partitions of
                true ->
                    % Update consumer group offset
                    ConsumerGroups = State#server_state.consumer_groups,
                    Group = maps:get(GroupId, ConsumerGroups, #consumer_group{id = GroupId}),
                    
                    % Set new offset
                    NewOffsets = maps:put({Topic, Partition}, get_current_offset(Topic, Partition, State#server_state.data_dir), Group#consumer_group.offsets),
                    NewGroup = Group#consumer_group{offsets = NewOffsets},
                    
                    % Update consumer groups
                    NewConsumerGroups = maps:put(GroupId, NewGroup, ConsumerGroups),
                    NewState = State#server_state{consumer_groups = NewConsumerGroups},
                    
                    % Save to disk
                    save_consumer_groups_to_disk(NewConsumerGroups, State#server_state.data_dir),
                    
                    {ok, NewState};
                false ->
                    {{error, invalid_partition}, State}
            end;
        error ->
            {{error, topic_not_found}, State}
    end;

handle_call(Request, State) ->
    {{error, unknown_request}, State}.

%% Handle asynchronous casts
handle_cast(Request, State) ->
    io:format("Unknown cast: ~p~n", [Request]),
    State.

%% Clean shutdown
shutdown(State) ->
    % Close RocksDB instances
    [close_partition_db(Topic, P, State#server_state.data_dir) || 
        {Topic, TopicInfo} <- maps:to_list(State#server_state.topics),
        P <- lists:seq(0, TopicInfo#topic.partitions-1)],
    
    % Stop Raft
    ra:stop_server(ekafka_raft),
    
    % Log shutdown
    io:format("Erlang Kafka stopped~n").

%% ===================================================================
%% RocksDB functions
%% ===================================================================

%% Creates a partition
create_partition(Topic, Partition, DataDir) ->
    PartitionPath = partition_path(Topic, Partition, DataDir),
    ok = filelib:ensure_dir(PartitionPath ++ "/"),
    
    % Create RocksDB instance
    DbOpts = [{create_if_missing, true}],
    {ok, Db} = rocksdb:open(PartitionPath, DbOpts),
    
    % Initialize offset counter
    rocksdb:put(Db, <<"offset">>, term_to_binary(0), []),
    
    % Close DB
    rocksdb:close(Db).

%% Deletes a partition
delete_partition(Topic, Partition, DataDir) ->
    PartitionPath = partition_path(Topic, Partition, DataDir),
    
    % Close DB if open
    close_partition_db(Topic, Partition, DataDir),
    
    % Delete files
    rocksdb:destroy(PartitionPath, []).

%% Opens a partition's RocksDB
open_partition_db(Topic, Partition, DataDir) ->
    PartitionPath = partition_path(Topic, Partition, DataDir),
    DbOpts = [{create_if_missing, true}],
    {ok, Db} = rocksdb:open(PartitionPath, DbOpts),
    Db.

%% Closes a partition's RocksDB
close_partition_db(Topic, Partition, DataDir) ->
    PartitionPath = partition_path(Topic, Partition, DataDir),
    case get({db, PartitionPath}) of
        undefined -> ok;
        Db ->
            rocksdb:close(Db),
            erase({db, PartitionPath})
    end.

%% Gets the current offset for a partition
get_current_offset(Topic, Partition, DataDir) ->
    Db = get_partition_db(Topic, Partition, DataDir),
    case rocksdb:get(Db, <<"offset">>, []) of
        {ok, BinOffset} -> binary_to_term(BinOffset);
        _ -> 0
    end.

%% Updates the offset for a partition
update_offset(Topic, Partition, NewOffset, DataDir) ->
    Db = get_partition_db(Topic, Partition, DataDir),
    rocksdb:put(Db, <<"offset">>, term_to_binary(NewOffset), []).

%% Writes a message to the database
write_message_to_db(Topic, Partition, Message, DataDir) ->
    Db = get_partition_db(Topic, Partition, DataDir),
    Key = <<"msg_", (integer_to_binary(Message#message.offset))/binary>>,
    
    % Serialize message
    BinMessage = term_to_binary(Message),
    
    % Write to RocksDB
    case rocksdb:put(Db, Key, BinMessage, []) of
        ok -> {ok, Message#message.offset};
        Error -> Error
    end.

%% Reads messages from the database
read_message_from_db(Topic, Partition, MaxMessages, DataDir) ->
    Db = get_partition_db(Topic, Partition, DataDir),
    
    % Get current consumer position
    % In a real system this would be tracked per consumer group
    ConsumerOffset = get_consumer_offset(Topic, Partition, DataDir),
    
    % Get messages
    Messages = read_messages(Db, ConsumerOffset, MaxMessages),
    
    % Update consumer offset if messages were read
    case Messages of
        [] -> 
            {ok, []};
        _ ->
            LastMessage = lists:last(Messages),
            LastOffset = LastMessage#message.offset,
            update_consumer_offset(Topic, Partition, LastOffset + 1, DataDir),
            {ok, Messages}
    end.

%% Read multiple messages starting from an offset
read_messages(Db, StartOffset, MaxMessages) ->
    % Create iterator
    {ok, Iter} = rocksdb:iterator(Db, []),
    
    % Seek to start position
    StartKey = <<"msg_", (integer_to_binary(StartOffset))/binary>>,
    rocksdb:iterator_move(Iter, {seek, StartKey}),
    
    % Read messages
    Messages = read_messages_iter(Iter, MaxMessages, []),
    
    % Close iterator
    rocksdb:iterator_close(Iter),
    
    lists:reverse(Messages).

%% Helper for reading messages with iterator
read_messages_iter(_, 0, Acc) -> 
    Acc;
read_messages_iter(Iter, Count, Acc) ->
    case rocksdb:iterator_move(Iter, next) of
        {ok, <<"msg_", _/binary>>, Value} ->
            Message = binary_to_term(Value),
            read_messages_iter(Iter, Count - 1, [Message | Acc]);
        _ ->
            Acc
    end.

%% Gets the consumer offset for a partition
get_consumer_offset(Topic, Partition, DataDir) ->
    Db = get_partition_db(Topic, Partition, DataDir),
    case rocksdb:get(Db, <<"consumer_offset">>, []) of
        {ok, BinOffset} -> binary_to_term(BinOffset);
        _ -> 0
    end.

%% Updates the consumer offset for a partition
update_consumer_offset(Topic, Partition, NewOffset, DataDir) ->
    Db = get_partition_db(Topic, Partition, DataDir),
    rocksdb:put(Db, <<"consumer_offset">>, term_to_binary(NewOffset), []).

%% Gets or opens a partition database
get_partition_db(Topic, Partition, DataDir) ->
    PartitionPath = partition_path(Topic, Partition, DataDir),
    case get({db, PartitionPath}) of
        undefined ->
            Db = open_partition_db(Topic, Partition, DataDir),
            put({db, PartitionPath}, Db),
            Db;
        Db ->
            Db
    end.

%% Gets the path for a partition
partition_path(Topic, Partition, DataDir) ->
    filename:join([DataDir, Topic, integer_to_list(Partition)]).

%% ===================================================================
%% Raft functions
%% ===================================================================

%% Initializes the Raft cluster
init_raft_cluster(NodeId, Peers) ->
    % Define server ID - Using atoms instead of direct node references to avoid unicode issues
    ServerId = {ekafka_raft, erlang:phash2(NodeId)},
    
    % Create cluster configuration
    ClusterName = ekafka_cluster,
    RaftConfig = #{
        id => ServerId,
        uid => erlang:phash2(ServerId),
        cluster_name => ClusterName,
        log_init_args => #{
            uid => erlang:phash2(ServerId)
        },
        initial_members => [{ekafka_raft, erlang:phash2(P)} || P <- Peers],
        machine => {module, ?MODULE, #{}}
    },
    
    io:format("Starting Raft with config: ~p~n", [RaftConfig]),
    
    % Start Raft server
    case ra:start_server(RaftConfig) of
        ok -> ok;
        {error, {already_started, _}} -> ok;
        Error -> 
            io:format("Error starting Raft server: ~p~n", [Error]),
            Error
    end,
    
    % Start cluster
    case Peers of
        [] ->
            % Single node cluster
            case ra:start_cluster(ClusterName, RaftConfig, [ServerId]) of
                {ok, _, _} -> ok;
                {error, {already_started, _}} -> ok;
                ClusterError -> 
                    io:format("Error starting Raft cluster: ~p~n", [ClusterError]),
                    ClusterError
            end;
        [FirstPeer|_] when NodeId =:= FirstPeer ->
            % Primary node starts the cluster
            PeerServerIds = [{ekafka_raft, erlang:phash2(P)} || P <- Peers],
            case ra:start_cluster(ClusterName, RaftConfig, PeerServerIds) of
                {ok, _, _} -> ok;
                {error, {already_started, _}} -> ok;
                ClusterError -> 
                    io:format("Error starting Raft cluster: ~p~n", [ClusterError]),
                    ClusterError
            end;
        _ ->
            % Other nodes join the cluster
            ok
    end.

%% Proposes a command to the Raft cluster
propose_to_raft(Command) ->
    ServerId = {ekafka_raft, erlang:phash2(node())},
    case ra:process_command(ServerId, Command) of
        {ok, Result, _} -> {ok, Result};
        {timeout, _} -> {error, timeout};
        Error -> Error
    end.

%% Apply a committed Raft command
apply_raft_command({create_topic, Topic}, State) ->
    % Update topics map
    NewTopics = maps:put(Topic#topic.name, Topic, State#server_state.topics),
    State#server_state{topics = NewTopics};

apply_raft_command({delete_topic, TopicName}, State) ->
    % Update topics map
    NewTopics = maps:remove(TopicName, State#server_state.topics),
    State#server_state{topics = NewTopics};

apply_raft_command({update_partition_leader, Topic, Partition, Leader}, State) ->
    % Update partition leader
    case maps:find(Topic, State#server_state.topics) of
        {ok, TopicInfo} ->
            NewLeaders = maps:put(Partition, Leader, TopicInfo#topic.partition_leaders),
            NewTopic = TopicInfo#topic{partition_leaders = NewLeaders},
            NewTopics = maps:put(Topic, NewTopic, State#server_state.topics),
            State#server_state{topics = NewTopics};
        error ->
            State
    end;

apply_raft_command(Command, State) ->
    io:format("Unknown Raft command: ~p~n", [Command]),
    State.

%% ===================================================================
%% Persistent state management
%% ===================================================================

%% Saves topics to disk
save_topics_to_disk(Topics, DataDir) ->
    Filename = filename:join(DataDir, "topics.dat"),
    ok = file:write_file(Filename, term_to_binary(Topics)).

%% Loads topics from disk
load_topics_from_disk(DataDir) ->
    Filename = filename:join(DataDir, "topics.dat"),
    case file:read_file(Filename) of
        {ok, Binary} -> binary_to_term(Binary);
        _ -> #{}
    end.

%% Saves consumer groups to disk
save_consumer_groups_to_disk(Groups, DataDir) ->
    Filename = filename:join(DataDir, "consumer_groups.dat"),
    ok = file:write_file(Filename, term_to_binary(Groups)).

%% Loads consumer groups from disk
load_consumer_groups_from_disk(DataDir) ->
    Filename = filename:join(DataDir, "consumer_groups.dat"),
    case file:read_file(Filename) of
        {ok, Binary} -> binary_to_term(Binary);
        _ -> #{}
    end.

%% ===================================================================
%% Helper functions
%% ===================================================================

%% Updates the server state
update_server_state(Fun) ->
    ekafka_server ! {cast, {update_state, Fun}}.

%% Makes a synchronous call to the server
call_server(Request) ->
    case whereis(ekafka_server) of
        undefined -> {error, not_running};
        Pid ->
            Pid ! {call, self(), Request},
            receive
                {ekafka_reply, Reply} -> Reply
            after 5000 ->
                {error, timeout}
            end
    end.

%% Raft machine callbacks for ra library
init(_Config) -> #{}.

apply(_Meta, {create_topic, Topic}, State) ->
    {State, {ok, Topic}, []};

apply(_Meta, {delete_topic, Name}, State) ->
    {State, {ok, Name}, []};

apply(_Meta, Command, State) ->
    {State, {error, {unknown_command, Command}}, []}.

%% State machine projection for ra library
state_enter(_StateName, _State) -> ok.