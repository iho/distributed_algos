-module(tigerbeetle).
-export([start/0, create_transfer/3, get_balance/1, stop/0]).

-record(state,
        {db,
         accounts = #{},
         pending = queue:new(),
         replicas = [],
         view = 1,
         op_next = 1,
         prepared = #{},
         commit_max = 0}).
-record(transfer,
        {id :: integer(),
         from :: integer(),
         to :: integer(),
         amount :: integer(),
         checksum :: integer(),
         timestamp :: integer()}).
-record(account, {id :: integer(), balance = 0 :: integer(), checksum :: integer()}).

%% Client API

start() ->
    Pid = spawn(fun() ->
                   case rocksdb:open("primary_db", [{create_if_missing, true}]) of
                       {ok, DB} ->
                           io:format("Primary DB opened~n", []),
                           Replicas = [spawn(fun() -> replica_start(I, self()) end) || I <- [1, 2, 3, 4, 5, 6]],
                           [io:format("Replica ~p spawned with PID ~p~n", [I, R]) || {I, R} <- lists:zip([1, 2, 3, 4, 5, 6], Replicas)],
                           InitialState =
                               #state{db = DB,
                                      accounts = #{},
                                      pending = queue:new(),
                                      replicas = Replicas,
                                      view = 1,
                                      op_next = 1,
                                      prepared = #{}},
                           register(tigerbeetle, self()),
                           io:format("Primary registered as tigerbeetle~n", []),
                           loop(recover_state(InitialState));
                       {error, Reason} ->
                           io:format("Primary DB open failed: ~p~n", [Reason]),
                           exit({db_open_failed, Reason})
                   end
                end),
    {ok, Pid}.

create_transfer(From, To, Amount) ->
    tigerbeetle ! {create_transfer, From, To, Amount, self()},
    receive
        {ok, TransferId} ->
            {ok, TransferId}
    after 5000 ->
        {error, timeout}
    end.

get_balance(AccountId) ->
    tigerbeetle ! {get_balance, AccountId, self()},
    receive
        {balance, Balance} ->
            {ok, Balance}
    after 5000 ->
        {error, timeout}
    end.

stop() ->
    tigerbeetle ! {stop, self()},
    receive
        stopped ->
            ok
    after 5000 ->
        {error, timeout}
    end.

%% Primary Process

loop(State =
         #state{db = DB,
                accounts = Accounts,
                pending = Pending,
                replicas = Replicas,
                view = View,
                op_next = OpNext,
                prepared = Prepared,
                commit_max = CommitMax}) ->
    receive
        {create_transfer, From, To, Amount, ReplyTo} ->
            io:format("Primary received create_transfer: ~p, ~p, ~p~n", [From, To, Amount]),
            Transfer0 =
                #transfer{id = OpNext,
                          from = From,
                          to = To,
                          amount = Amount,
                          timestamp = erlang:system_time(second),
                          checksum = 0},
            Checksum = erlang:crc32(term_to_binary(Transfer0)),
            Transfer = Transfer0#transfer{checksum = Checksum},
            case rocksdb:put(DB, term_to_binary({transfer, OpNext}), term_to_binary(Transfer), [{sync, true}]) of
                ok ->
                    io:format("Primary stored transfer ~p~n", [OpNext]),
                    NewPending = queue:in({Transfer, ReplyTo}, Pending),
                    [R ! {prepare, View, OpNext, Transfer, self()} || R <- Replicas],
                    io:format("Primary sent prepare for op ~p to ~p replicas~n", [OpNext, length(Replicas)]),
                    loop(State#state{pending = NewPending, op_next = OpNext + 1});
                {error, Reason} ->
                    io:format("Primary RocksDB put failed: ~p~n", [Reason]),
                    exit({rocksdb_error, Reason})
            end;
        {prepare_ok, OpNumber, ReplicaPid} ->
            io:format("Primary received prepare_ok for op ~p from ~p~n", [OpNumber, ReplicaPid]),
            NewState = handle_prepare_ok(OpNumber, ReplicaPid, State),
            loop(NewState);
        {commit, OpNumber} ->
            io:format("Primary committing op ~p~n", [OpNumber]),
            NewState = commit_operations(OpNumber, State),
            loop(NewState);
        {get_balance, AccountId, ReplyTo} ->
            Balance =
                case maps:get(AccountId, Accounts, undefined) of
                    undefined ->
                        0;
                    #account{balance = B} ->
                        B
                end,
            ReplyTo ! {balance, Balance},
            loop(State);
        {stop, ReplyTo} ->
            [R ! {stop, self()} || R <- Replicas],
            lists:foreach(fun(_) -> receive {replica_stopped, _} -> ok end end, Replicas),
            rocksdb:close(DB),
            ReplyTo ! stopped,
            ok
    end.

%% Helper Functions

handle_prepare_ok(OpNumber,
                  ReplicaPid,
                  State =
                      #state{prepared = Prepared,
                             commit_max = CommitMax,
                             replicas = Replicas}) ->
    UpdatedPrepared =
        maps:update_with(OpNumber,
                         fun(Acks) -> Acks#{ReplicaPid => ok} end,
                         #{ReplicaPid => ok},
                         Prepared),
    NewState = State#state{prepared = UpdatedPrepared},
    check_commit(NewState).

check_commit(State =
                 #state{prepared = Prepared,
                        commit_max = CommitMax,
                        replicas = Replicas}) ->
    Majority = length(Replicas) div 2 + 1,
    NextOp = CommitMax + 1,
    case maps:get(NextOp, Prepared, #{}) of
        Acks when map_size(Acks) >= Majority ->
            self() ! {commit, NextOp},
            check_commit(State#state{commit_max = NextOp});
        _ ->
            State
    end.

commit_operations(OpNumber,
                  State =
                      #state{pending = Pending,
                             accounts = Accounts,
                             replicas = Replicas,
                             commit_max = CommitMax}) ->
    case queue:peek(Pending) of
        empty ->
            State;
        {value, {Transfer, ReplyTo}} ->
            if Transfer#transfer.id =< OpNumber ->
                   NewPending = queue:drop(Pending),
                   NewAccounts = apply_transfer(Transfer, Accounts),
                   ReplyTo ! {ok, Transfer#transfer.id},
                   [R ! {commit, State#state.view, Transfer#transfer.id, Transfer}
                    || R <- Replicas],
                   commit_operations(OpNumber,
                                     State#state{pending = NewPending,
                                                 accounts = NewAccounts,
                                                 commit_max =
                                                     max(CommitMax, Transfer#transfer.id)});
               true ->
                   State
            end
    end.

apply_transfer(Transfer, Accounts) ->
    FromAccount =
        maps:get(Transfer#transfer.from, Accounts, #account{id = Transfer#transfer.from}),
    ToAccount = maps:get(Transfer#transfer.to, Accounts, #account{id = Transfer#transfer.to}),
    NewFrom0 =
        FromAccount#account{balance = FromAccount#account.balance - Transfer#transfer.amount},
    NewFrom = NewFrom0#account{checksum = erlang:crc32(term_to_binary(NewFrom0))},
    NewTo0 =
        ToAccount#account{balance = ToAccount#account.balance + Transfer#transfer.amount},
    NewTo = NewTo0#account{checksum = erlang:crc32(term_to_binary(NewTo0))},
    Accounts#{Transfer#transfer.from => NewFrom, Transfer#transfer.to => NewTo}.

recover_state(State = #state{db = DB, accounts = Accounts}) ->
    HighestOp = find_highest_op(DB),
    NewAccounts =
        lists:foldl(fun(OpNum, Acc) ->
                       {ok, Bin} = rocksdb:get(DB, term_to_binary({transfer, OpNum}), []),
                       Transfer = binary_to_term(Bin),
                       apply_transfer(Transfer, Acc)
                    end,
                    Accounts,
                    lists:seq(1, HighestOp)),
    State#state{accounts = NewAccounts, op_next = HighestOp + 1}.

find_highest_op(DB) ->
    {ok, Iter} = rocksdb:iterator(DB, []),
    Highest = find_highest_op_loop(Iter, rocksdb:iterator_move(Iter, first), 0),
    rocksdb:iterator_close(Iter),
    Highest.

find_highest_op_loop(_Iter, {error, invalid_iterator}, Highest) ->
    Highest;
find_highest_op_loop(Iter, {ok, KeyBin, _}, Highest) ->
    Key = catch binary_to_term(KeyBin),
    NewHighest =
        case Key of
            {transfer, OpNum} when is_integer(OpNum) ->
                max(Highest, OpNum);
            _ ->
                Highest
        end,
    find_highest_op_loop(Iter, rocksdb:iterator_move(Iter, next), NewHighest).

%% Replica Process

replica_start(Id, Primary) ->
    DBPath = "replica_" ++ integer_to_list(Id) ++ "_db",
    case rocksdb:open(DBPath, [{create_if_missing, true}]) of
        {ok, DB} ->
            io:format("Replica ~p started~n", [Id]),
            replica_loop(Primary, DB, #{});
        {error, Reason} ->
            io:format("Replica ~p failed to open DB: ~p~n", [Id, Reason]),
            exit({db_open_failed, Reason})
    end.

replica_loop(Primary, DB, State) ->
    receive
        {prepare, View, OpNumber, Transfer, From} ->
            io:format("Replica ~p received prepare for op ~p~n", [self(), OpNumber]),
            case rocksdb:put(DB, term_to_binary({log, OpNumber}), term_to_binary(Transfer), [{sync, true}]) of
                ok ->
                    io:format("Replica ~p stored op ~p~n", [self(), OpNumber]),
                    From ! {prepare_ok, OpNumber, self()},
                    replica_loop(Primary, DB, State);
                {error, Reason} ->
                    io:format("Replica ~p RocksDB put failed: ~p~n", [self(), Reason]),
                    exit({rocksdb_error, Reason})
            end;
        {commit, _View, CommitNumber, Transfer} ->
            io:format("Replica ~p received commit for op ~p~n", [self(), CommitNumber]),
            NewState = apply_transfer(Transfer, State),
            replica_loop(Primary, DB, NewState);
        {stop, ReplyTo} ->
            rocksdb:close(DB),
            ReplyTo ! {replica_stopped, self()},
            ok
    end.