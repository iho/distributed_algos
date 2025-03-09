-module(range_machine).

-behaviour(ra_machine).

-export([init/1, apply/3]).

%% State record for the range
-record(state, {
    committed = #{} :: #{binary() => [{integer(), binary()}]},  % Key -> [{Timestamp, Value}]
    intents = #{} :: #{binary() => {binary(), binary()}},      % Key -> {TxnId, Value}
    txns = #{} :: #{binary() => {atom(), integer(), [binary()]}}, % TxnId -> {State, StartTs, WrittenKeys}
    next_txn_id = 0 :: integer(),                              % Next transaction ID
    last_ts = 0 :: integer()                                   % Last timestamp used
}).

%% Initialize the state
init(_Config) ->
    #state{}.

%% Apply Raft commands to the state machine
apply(_Meta, Command, State) ->
    case Command of
        %% Start a new transaction
        {start_txn, ReplyTo} ->
            TxnId = integer_to_binary(State#state.next_txn_id),
            StartTs = State#state.last_ts + 1,
            NewTxns = maps:put(TxnId, {pending, StartTs, []}, State#state.txns),
            NewState = State#state{
                txns = NewTxns,
                next_txn_id = State#state.next_txn_id + 1,
                last_ts = StartTs
            },
            {NewState, [{reply, ReplyTo, {ok, TxnId, StartTs}}]};

        %% Put a value for a key within a transaction
        {put, TxnId, Key, Value, ReplyTo} ->
            case maps:get(TxnId, State#state.txns, undefined) of
                {pending, StartTs, WrittenKeys} ->
                    case maps:get(Key, State#state.intents, undefined) of
                        undefined ->
                            NewIntents = maps:put(Key, {TxnId, Value}, State#state.intents),
                            NewWrittenKeys = [Key | WrittenKeys],
                            NewTxns = maps:put(TxnId, {pending, StartTs, NewWrittenKeys}, State#state.txns),
                            NewState = State#state{intents = NewIntents, txns = NewTxns},
                            {NewState, [{reply, ReplyTo, ok}]};
                        {OtherTxnId, _} when OtherTxnId /= TxnId ->
                            {State, [{reply, ReplyTo, {error, conflict}}]};
                        {TxnId, _} ->
                            %% Overwrite own intent
                            NewIntents = maps:put(Key, {TxnId, Value}, State#state.intents),
                            NewState = State#state{intents = NewIntents},
                            {NewState, [{reply, ReplyTo, ok}]}
                    end;
                _ ->
                    {State, [{reply, ReplyTo, {error, invalid_txn}}]}
            end;

        %% Get the value for a key within a transaction
        {get, TxnId, Key, ReplyTo} ->
            case maps:get(TxnId, State#state.txns, undefined) of
                {pending, StartTs, _} ->
                    case maps:get(Key, State#state.intents, undefined) of
                        {TxnId, Value} ->
                            {State, [{reply, ReplyTo, {ok, Value}}]};
                        _ ->
                            Versions = maps:get(Key, State#state.committed, []),
                            VisibleVersions = [V || {Ts, V} <- Versions, Ts =< StartTs],
                            case VisibleVersions of
                                [] -> {State, [{reply, ReplyTo, {ok, undefined}}]};
                                [Latest | _] -> {State, [{reply, ReplyTo, {ok, Latest}}]}
                            end
                    end;
                _ ->
                    {State, [{reply, ReplyTo, {error, invalid_txn}}]}
            end;

        %% Commit a transaction
        {commit_txn, TxnId, ReplyTo} ->
            case maps:get(TxnId, State#state.txns, undefined) of
                {pending, StartTs, WrittenKeys} ->
                    Conflicts = [Key || Key <- WrittenKeys, has_conflict(Key, StartTs, State)],
                    if
                        Conflicts == [] ->
                            CommitTs = State#state.last_ts + 1,
                            NewCommitted = lists:foldl(
                                fun(Key, Acc) ->
                                    {_, Value} = maps:get(Key, State#state.intents),
                                    Versions = maps:get(Key, Acc, []),
                                    maps:put(Key, [{CommitTs, Value} | Versions], Acc)
                                end,
                                State#state.committed,
                                WrittenKeys
                            ),
                            NewIntents = maps:without(WrittenKeys, State#state.intents),
                            NewTxns = maps:put(TxnId, {committed, StartTs, CommitTs}, State#state.txns),
                            NewState = State#state{
                                committed = NewCommitted,
                                intents = NewIntents,
                                txns = NewTxns,
                                last_ts = CommitTs
                            },
                            {NewState, [{reply, ReplyTo, ok}]};
                        true ->
                            NewIntents = maps:without(WrittenKeys, State#state.intents),
                            NewTxns = maps:put(TxnId, {aborted, StartTs}, State#state.txns),
                            NewState = State#state{intents = NewIntents, txns = NewTxns},
                            {NewState, [{reply, ReplyTo, {error, conflict}}]}
                    end;
                _ ->
                    {State, [{reply, ReplyTo, {error, invalid_txn}}]}
            end;

        %% Abort a transaction
        {abort_txn, TxnId, ReplyTo} ->
            case maps:get(TxnId, State#state.txns, undefined) of
                {pending, StartTs, WrittenKeys} ->
                    NewIntents = maps:without(WrittenKeys, State#state.intents),
                    NewTxns = maps:put(TxnId, {aborted, StartTs}, State#state.txns),
                    NewState = State#state{intents = NewIntents, txns = NewTxns},
                    {NewState, [{reply, ReplyTo, ok}]};
                _ ->
                    {State, [{reply, ReplyTo, {error, invalid_txn}}]}
            end
    end.

%% Helper function to detect conflicts
has_conflict(Key, StartTs, State) ->
    Versions = maps:get(Key, State#state.committed, []),
    case Versions of
        [] -> false;
        [{LatestTs, _} | _] -> LatestTs > StartTs
    end.