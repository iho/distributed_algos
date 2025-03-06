-module(tigerbeetle).
-export([start/0, create_transfer/3, get_balance/1, stop/0]).

% Record definitions
-record(transfer, {id :: integer(), from :: integer(), to :: integer(), amount :: integer(), timestamp :: integer(), checksum :: integer()}).
-record(account, {id :: integer(), balance = 0 :: integer(), checksum :: integer()}).
-record(state, {
    db :: rocksdb:db_handle(),
    accounts :: map(),
    pending :: queue:queue(),
    commit_max = 0 :: integer(),
    replicas = [] :: [pid()],
    view = 1 :: integer(),
    op_next = 1 :: integer(),
    prepared = #{} :: map()
}).

% Start the server
start() ->
    Pid = spawn(fun() ->
        {ok, DB} = rocksdb:open("tigerbeetle_db", [{create_if_missing, true}]),
        Replicas = spawn_replicas(2, self()),
        InitialState = #state{
            db = DB,
            accounts = #{},
            pending = queue:new(),
            replicas = Replicas,
            view = 1,
            op_next = 1,
            prepared = #{}
        },
        register(tigerbeetle, self()),
        loop(recover_state(InitialState))
    end),
    {ok, Pid}.

% Main server loop (primary)
loop(State) ->
    receive
        {create_transfer, From, To, Amount, ReplyTo} ->
            {NewState, OpNumber} = queue_transfer(From, To, Amount, ReplyTo, State),
            loop(NewState);
        {prepare_ok, OpNumber, ReplicaPid} ->
            NewState = handle_prepare_ok(OpNumber, ReplicaPid, State),
            loop(NewState);
        {commit, OpNumber} ->
            NewState = commit_operations(OpNumber, State),
            loop(NewState);
        {get_balance, AccountId, ReplyTo} ->
            Balance = get_account_balance(AccountId, State),
            ReplyTo ! {ok, Balance},
            loop(State);
        {stop, ReplyTo} ->
            [R ! {stop, self()} || R <- State#state.replicas],
            rocksdb:close(State#state.db),
            ReplyTo ! ok,
            ok
    end.

% Client API
create_transfer(From, To, Amount) ->
    tigerbeetle ! {create_transfer, From, To, Amount, self()},
    receive
        {ok, TransferId} -> {ok, TransferId}
    after 5000 ->
        {error, timeout}
    end.

get_balance(AccountId) ->
    tigerbeetle ! {get_balance, AccountId, self()},
    receive
        {ok, Balance} -> {ok, Balance}
    after 5000 ->
        {error, timeout}
    end.

stop() ->
    tigerbeetle ! {stop, self()},
    receive
        ok -> ok
    after 5000 ->
        {error, timeout}
    end.

% Queue a transfer operation (primary)
queue_transfer(From, To, Amount, ReplyTo, State = #state{db = DB, pending = Pending, op_next = OpNext, replicas = Replicas, view = View}) ->
    Transfer = #transfer{id = OpNext, from = From, to = To, amount = Amount, timestamp = erlang:system_time(second), checksum = 0},
    ok = rocksdb:put(DB, term_to_binary({transfer, OpNext}), term_to_binary(Transfer), [{sync, true}]),
    NewPending = queue:in({Transfer, ReplyTo}, Pending),
    [R ! {prepare, View, OpNext, Transfer, self()} || R <- Replicas],
    {State#state{pending = NewPending, op_next = OpNext + 1}, OpNext}.

% Handle prepare acknowledgment from replicas
handle_prepare_ok(OpNumber, ReplicaPid, State = #state{replicas = Replicas, prepared = Prepared}) ->
    UpdatedPrepared = maps:update_with(OpNumber, 
        fun(Acks) -> Acks#{ReplicaPid => ok} end,
        #{ReplicaPid => ok},
        Prepared),
    case maps:size(maps:get(OpNumber, UpdatedPrepared, #{})) >= 2 of
        true ->
            self() ! {commit, OpNumber},
            State#state{prepared = UpdatedPrepared};
        false ->
            State#state{prepared = UpdatedPrepared}
    end.

% Commit operations up to OpNumber
commit_operations(OpNumber, State = #state{db = DB, pending = Pending, replicas = Replicas, view = View}) ->
    case queue:peek(Pending) of
        empty ->
            State;
        {value, {Transfer, ReplyTo}} ->
            if
                Transfer#transfer.id =< OpNumber ->
                    NewPending = queue:drop(Pending),
                    NewState = apply_transfer(Transfer, State),
                    ReplyTo ! {ok, Transfer#transfer.id},
                    [R ! {commit, View, OpNumber, Transfer} || R <- Replicas],
                    commit_operations(OpNumber, NewState#state{
                        pending = NewPending,
                        commit_max = max(State#state.commit_max, Transfer#transfer.id)
                    });
                true ->
                    State
            end
    end.

% Apply transfer to state and persist to RocksDB
apply_transfer(Transfer, State = #state{db = DB, accounts = Accounts}) ->
    FromAccount = maps:get(Transfer#transfer.from, Accounts, #account{id = Transfer#transfer.from}),
    ToAccount = maps:get(Transfer#transfer.to, Accounts, #account{id = Transfer#transfer.to}),
    NewFrom = FromAccount#account{
        balance = FromAccount#account.balance - Transfer#transfer.amount,
        checksum = calculate_checksum(FromAccount)
    },
    NewTo = ToAccount#account{
        balance = ToAccount#account.balance + Transfer#transfer.amount,
        checksum = calculate_checksum(ToAccount)
    },
    NewAccounts = Accounts#{Transfer#transfer.from => NewFrom, Transfer#transfer.to => NewTo},
    ok = rocksdb:put(DB, <<"accounts">>, term_to_binary(NewAccounts), [{sync, true}]),
    State#state{accounts = NewAccounts}.

% Get account balance
get_account_balance(AccountId, #state{accounts = Accounts}) ->
    case maps:get(AccountId, Accounts, undefined) of
        undefined -> 0;
        Account -> Account#account.balance
    end.

% Recover state from RocksDB
recover_state(State = #state{db = DB}) ->
    % Load accounts
    Accounts = case rocksdb:get(DB, <<"accounts">>, []) of
        {ok, Binary} -> binary_to_term(Binary);
        not_found -> #{};
        {error, Reason} -> error({rocksdb_recovery_failed, Reason})
    end,
    % Find highest committed operation number
    CommitMax = case find_highest_op(DB) of
        {ok, Max} -> Max;
        not_found -> 0
    end,
    State#state{accounts = Accounts, commit_max = CommitMax, op_next = CommitMax + 1}.

% Find highest committed operation number in RocksDB
find_highest_op(DB) ->
    {ok, Iterator} = rocksdb:iterator(DB, []),
    Highest = find_highest_op_loop(rocksdb:iterator_move(Iterator, first), Iterator, 0),
    rocksdb:iterator_close(Iterator),
    case Highest of
        0 -> not_found;
        N -> {ok, N}
    end.

find_highest_op_loop({ok, Key, _Value}, Iterator, Max) ->
    % Safely handle keys that are not valid terms or do not match {transfer, Op}
    case is_binary(Key) andalso (catch binary_to_term(Key)) of
        {transfer, Op} when is_integer(Op) andalso Op > Max ->
            find_highest_op_loop(rocksdb:iterator_move(Iterator, next), Iterator, Op);
        _ ->
            find_highest_op_loop(rocksdb:iterator_move(Iterator, next), Iterator, Max)
    end;
find_highest_op_loop({error, invalid_iterator}, _Iterator, Max) ->
    Max.

% Replication (replica process)
spawn_replicas(Count, Primary) ->
    [spawn(fun() -> replica_loop(Primary) end) || _ <- lists:seq(1, Count)].

replica_loop(Primary) ->
    receive
        {prepare, View, OpNumber, Transfer, From} ->
            From ! {prepare_ok, OpNumber, self()},
            replica_loop(Primary);
        {commit, _View, OpNumber, Transfer} ->
            replica_loop(Primary);
        {stop, ReplyTo} ->
            ReplyTo ! {replica_stopped, self()},
            ok
    end.

% Helper
calculate_checksum(Record) ->
    erlang:crc32(term_to_binary(Record)).