-module(db_router).

-export([start/0, start_txn/1, put/3, get/2, commit_txn/1, abort_txn/1]).


start() ->
    Nodes = [{node1, node()}, {node2, 'node2@host'}, {node3, 'node3@host'}], % Adjust as per your setup
    ClusterOptions = #{leader_timeout => 5000, log_init_args => #{}},
    ra:start_cluster(range1, Nodes, {module, range_machine, #{}}, ClusterOptions, 5000).

%% Map a key to a range
get_range_for_key(Key) ->
    case binary:first(Key) of
        C when C =< $m -> range1;
        _ -> range2
    end.

%% Start a transaction for a key
start_txn(Key) ->
    RangeId = get_range_for_key(Key),
    {ok, CorrId} = ra:process_command(RangeId, {start_txn, self()}),
    receive
        {ra_event, _, {applied, {CorrId, {ok, TxnId, StartTs}}}} ->
            {ok, TxnId, RangeId}  % Return RangeId for subsequent operations
    after 5000 ->
        {error, timeout}
    end.

%% Put a value for a key in a transaction
put(TxnId, Key, Value) ->
    RangeId = get_range_for_key(Key),
    {ok, CorrId} = ra:process_command(RangeId, {put, TxnId, Key, Value, self()}),
    receive
        {ra_event, _, {applied, {CorrId, Reply}}} -> Reply
    after 5000 ->
        {error, timeout}
    end.

%% Get the value for a key in a transaction
get(TxnId, Key) ->
    RangeId = get_range_for_key(Key),
    {ok, CorrId} = ra:process_command(RangeId, {get, TxnId, Key, self()}),
    receive
        {ra_event, _, {applied, {CorrId, {ok, Value}}}} -> {ok, Value}
    after 5000 ->
        {error, timeout}
    end.

%% Commit a transaction
commit_txn({TxnId, RangeId}) ->
    {ok, CorrId} = ra:process_command(RangeId, {commit_txn, TxnId, self()}),
    receive
        {ra_event, _, {applied, {CorrId, Reply}}} -> Reply
    after 5000 ->
        {error, timeout}
    end.

%% Abort a transaction
abort_txn({TxnId, RangeId}) ->
    {ok, CorrId} = ra:process_command(RangeId, {abort_txn, TxnId, self()}),
    receive
        {ra_event, _, {applied, {CorrId, Reply}}} -> Reply
    after 5000 ->
        {error, timeout}
    end.