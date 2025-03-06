-module(tigerbeetle).
-export([start/0, create_transfer/3, get_balance/1, stop/0]).

% Record definitions
-record(transfer, {id :: integer(), from :: integer(), to :: integer(), amount :: integer(), timestamp :: integer(), checksum :: integer()}).
-record(account, {id :: integer(), balance = 0 :: integer(), checksum :: integer()}).
-record(state, {db :: rocksdb:db_handle(), accounts :: map(), pending :: queue:queue(), commit_max = 0 :: integer(), replicas = [] :: [pid()]}).

% Start the server
start() ->
    Pid = spawn(fun() ->
        % Open RocksDB database
        {ok, DB} = rocksdb:open("tigerbeetle_db", [{create_if_missing, true}]),
        InitialState = #state{
            db = DB,
            accounts = #{},
            pending = queue:new(),
            replicas = spawn_replicas(2)
        },
        register(tigerbeetle, self()),
        loop(recover_state(InitialState))
    end),
    {ok, Pid}.

% Main server loop
loop(State) ->
    receive
        {create_transfer, From, To, Amount, ReplyTo} ->
            {NewState, OpNumber} = queue_transfer(From, To, Amount, ReplyTo, State),
            self() ! {commit, OpNumber},
            loop(NewState);
        {commit, OpNumber} ->
            NewState = commit_operations(OpNumber, State),
            loop(NewState);
        {get_balance, AccountId, ReplyTo} ->
            Balance = get_account_balance(AccountId, State),
            ReplyTo ! {ok, Balance},
            loop(State);
        {replicate, Op, FromReplica} ->
            NewState = handle_replication(Op, FromReplica, State),
            loop(NewState);
        {stop, ReplyTo} ->
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

% Queue a transfer operation
queue_transfer(From, To, Amount, ReplyTo, State = #state{db = DB, pending = Pending}) ->
    OpNumber = State#state.commit_max + queue:len(Pending) + 1,
    Transfer = #transfer{id = OpNumber, from = From, to = To, amount = Amount, timestamp = erlang:system_time(second), checksum = 0},
    % Store in RocksDB
    ok = rocksdb:put(DB, term_to_binary(OpNumber), term_to_binary(Transfer), [{sync, true}]),
    NewPending = queue:in({Transfer, ReplyTo}, Pending),
    replicate_to_peers(State#state.replicas, {transfer, Transfer, OpNumber}),
    {State#state{pending = NewPending}, OpNumber}.

% Commit operations up to OpNumber
commit_operations(OpNumber, State = #state{pending = Pending}) ->
    case queue:peek(Pending) of
        empty ->
            State;
        {value, {Transfer, ReplyTo}} ->
            if
                Transfer#transfer.id =< OpNumber ->
                    NewPending = queue:drop(Pending),
                    NewState = apply_transfer(Transfer, State),
                    ReplyTo ! {ok, Transfer#transfer.id},
                    commit_operations(OpNumber, NewState#state{
                        pending = NewPending,
                        commit_max = max(State#state.commit_max, Transfer#transfer.id)
                    });
                true ->
                    State
            end
    end.

% Apply transfer to state (allow negative balances)
apply_transfer(Transfer, State = #state{accounts = Accounts}) ->
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
    State#state{accounts = Accounts#{Transfer#transfer.from => NewFrom, Transfer#transfer.to => NewTo}}.

% Get account balance
get_account_balance(AccountId, #state{accounts = Accounts}) ->
    case maps:get(AccountId, Accounts, undefined) of
        undefined -> 0;
        Account -> Account#account.balance
    end.

% Recovery
recover_state(State = #state{db = DB}) ->
    % Simplified recovery: load accounts from RocksDB if needed
    State.

% Replication (simplified)
spawn_replicas(Count) ->
    [spawn(fun() -> replica_loop(self()) end) || _ <- lists:seq(1, Count)].

replica_loop(Primary) ->
    receive
        {transfer, Transfer, OpNumber} ->
            Primary ! {replicate, {Transfer, OpNumber}, self()},
            replica_loop(Primary)
    end.

replicate_to_peers(Replicas, Op) ->
    [R ! Op || R <- Replicas].

handle_replication({Transfer, OpNumber}, _From, State = #state{db = DB}) ->
    ok = rocksdb:put(DB, term_to_binary(OpNumber), term_to_binary(Transfer), [{sync, true}]),
    State.

% Helper
calculate_checksum(Record) ->
    erlang:crc32(term_to_binary(Record)).