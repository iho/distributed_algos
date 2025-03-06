-module(tigerbeetle).
-export([start/1, create_transfers/1, get_balance/1, stop/0, reset/0, debug_accounts/0]).

%% Record Definitions
-record(transfer, {id, from, to, amount, timestamp, checksum}).
-record(account, {id, balance = 0, checksum}).
-record(batch, {id, transfers = [], checksum}).
-record(state, {
    db,                  % RocksDB handle
    accounts = #{},      % Current account states
    replicas = [],       % List of replica PIDs
    view = 1,            % Current view number
    op_next = 1,         % Next operation number
    prepared = #{},      % Prepared operations and acks
    commit_max = 0,      % Highest committed operation
    batch_size = 100,    % Max transfers per batch
    processed_ids = #{}  % Processed transfer IDs
}).

%% Client API

%% @doc Start the system with a given number of replicas.
start(ReplicaCount) ->
    Pid = spawn(fun() ->
        % Ensure DB directory exists
        filelib:ensure_dir("db/primary_db/"),
        
        % Open the primary database with proper options
        {ok, DB} = rocksdb:open("db/primary_db", [
            {create_if_missing, true},
            {error_if_exists, false},
            {paranoid_checks, false}
        ]),
        
        % Create replica processes
        Replicas = [spawn(fun() -> replica_start(I, self()) end) || I <- lists:seq(1, ReplicaCount)],
        
        % Register the process name and enter the loop
        register(tigerbeetle, self()),
        InitialState = #state{db = DB, replicas = Replicas},
        
        % Give replicas time to initialize
        timer:sleep(500),
        
        % Recover state and start the main loop
        loop(recover_state(InitialState))
    end),
    
    % Start the monitoring process
    spawn(fun() -> monitor_primary(Pid, ReplicaCount) end),
    
    % Return the process ID
    {ok, Pid}.

%% @doc Reset the database (for debugging)
reset() ->
    % Stop the system if it's running
    case whereis(tigerbeetle) of
        undefined -> ok;
        _ -> stop()
    end,
    
    % Delete all database files
    os:cmd("rm -rf db"),
    io:format("Database reset complete~n"),
    ok.

%% @doc Debug: Print all accounts and their balances
debug_accounts() ->
    tigerbeetle ! {debug_accounts, self()},
    receive
        {accounts, Accounts} ->
            [io:format("Account ~p: Balance ~p~n", [Id, Acc#account.balance]) || {Id, Acc} <- maps:to_list(Accounts)],
            {ok, maps:size(Accounts)}
    after 5000 ->
        {error, timeout}
    end.

%% @doc Create a batch of transfers.
create_transfers(Transfers) when is_list(Transfers) ->
    % Add additional validation on the structure of the transfers
    case verify_transfers_structure(Transfers) of
        ok ->
            BatchId = generate_batch_id(),
            tigerbeetle ! {create_batch, BatchId, Transfers, self()},
            receive
                {ok, BatchId} -> {ok, BatchId};
                {error, Reason} -> {error, Reason}
            after 5000 -> {error, timeout}
            end;
        {error, _Reason} = Error -> 
            Error
    end;
create_transfers(_) ->
    {error, invalid_transfers_format}.

%% Verify the correct structure of transfers
verify_transfers_structure(Transfers) ->
    try
        lists:foreach(fun(Transfer) ->
            case Transfer of
                {Id, From, To, Amount} when is_integer(Amount), Amount > 0 ->
                    ok;
                _ ->
                    throw({invalid_transfer_format, Transfer})
            end
        end, Transfers),
        ok
    catch
        throw:Reason ->
            {error, Reason}
    end.

%% @doc Get the balance of an account.
get_balance(AccountId) ->
    tigerbeetle ! {get_balance, AccountId, self()},
    receive
        {balance, Balance} -> {ok, Balance};
        {error, Reason} -> {error, Reason}
    after 5000 -> {error, timeout}
    end.

%% @doc Stop the system.
stop() ->
    tigerbeetle ! {stop, self()},
    receive stopped -> ok after 5000 -> {error, timeout} end.

%% Primary Process

loop(State = #state{db = DB, accounts = Accounts, replicas = Replicas, view = View, 
                    op_next = OpNext, prepared = Prepared, commit_max = CommitMax, 
                    batch_size = BatchSize, processed_ids = ProcessedIds}) ->
    receive
        {create_batch, BatchId, Transfers, ReplyTo} ->
            io:format("Creating batch with ~p transfers~n", [length(Transfers)]),
            
            case validate_transfers(Transfers, ProcessedIds) of
                {ok, ValidTransfers} ->
                    % Create and log the batch
                    Batch = create_batch(BatchId, ValidTransfers),
                    ok = log_batch(DB, Batch, OpNext),
                    
                    % Notify replicas
                    [R ! {prepare_batch, View, OpNext, Batch, self()} || R <- Replicas],
                    
                    % Update local state
                    NewProcessedIds = update_processed_ids(Transfers, ProcessedIds),
                    
                    % Apply transfers locally for durability - SINGLE APPLICATION ONLY!
                    OldAccounts = Accounts,  % For debugging
                    NewAccounts = apply_batch_once(Batch, Accounts),
                    
                    % Print changes for debugging
                    io:format("Account changes after batch:~n"),
                    print_account_changes(OldAccounts, NewAccounts),
                    
                    % Persist the state
                    persist_state(DB, NewAccounts),
                    
                    % Update state and reply
                    NewState = State#state{
                        op_next = OpNext + 1, 
                        prepared = Prepared#{OpNext => #{}}, 
                        processed_ids = NewProcessedIds,
                        accounts = NewAccounts
                    },
                    
                    ReplyTo ! {ok, BatchId},
                    loop(NewState);
                {error, Reason} ->
                    ReplyTo ! {error, Reason},
                    loop(State)
            end;

        {prepare_ok, OpNumber, ReplicaPid} ->
            NewPrepared = case maps:is_key(OpNumber, Prepared) of
                true -> 
                    ExistingAcks = maps:get(OpNumber, Prepared),
                    maps:put(OpNumber, ExistingAcks#{ReplicaPid => ok}, Prepared);
                false -> 
                    Prepared#{OpNumber => #{ReplicaPid => ok}}
            end,
            NewState = State#state{prepared = NewPrepared},
            case map_size(maps:get(OpNumber, NewPrepared)) >= quorum_size(Replicas) of
                true -> self() ! {commit, OpNumber};
                false -> ok
            end,
            loop(NewState);

        {commit, OpNumber} ->
            try
                Batch = get_batch(DB, OpNumber),
                
                % Apply the batch to the accounts - IMPORTANT: ONLY APPLY ONCE!
                % We don't apply here as we already applied in create_batch for durability
                % This just sets the commit status
                io:format("Committing batch ~p~n", [OpNumber]),
                
                % Notify replicas
                [R ! {commit_batch, View, OpNumber, Batch} || R <- Replicas],
                
                loop(State#state{commit_max = max(CommitMax, OpNumber)})
            catch
                error:{batch_not_found, OpNumber} ->
                    error_logger:error_msg("Batch not found for operation ~p", [OpNumber]),
                    loop(State)
            end;

        {get_balance, AccountId, ReplyTo} ->
            Balance = case maps:find(AccountId, Accounts) of
                {ok, Account} -> Account#account.balance;
                error -> 0
            end,
            ReplyTo ! {balance, Balance},
            loop(State);
            
        {debug_accounts, ReplyTo} ->
            ReplyTo ! {accounts, Accounts},
            loop(State);
            
        {get_view, ReplyTo} ->
            ReplyTo ! {view, View},
            loop(State);

        {view_change, NewView, NewPrimary} ->
            case NewPrimary =:= self() of
                true ->
                    NewState = State#state{view = NewView, prepared = #{}},
                    sync_replicas(NewState),
                    loop(NewState);
                false ->
                    rocksdb:close(DB),
                    exit(view_changed)
            end;

        {stop, ReplyTo} ->
            % Ensure state is persisted before stopping
            persist_state(DB, Accounts),
            
            [R ! {stop, self()} || R <- Replicas],
            rocksdb:close(DB),
            ReplyTo ! stopped,
            ok
    end.

% Helper to print account changes for debugging
print_account_changes(OldAccounts, NewAccounts) ->
    AllKeys = lists:usort(maps:keys(OldAccounts) ++ maps:keys(NewAccounts)),
    [begin
        OldBalance = case maps:find(K, OldAccounts) of
            {ok, OldAcc} -> OldAcc#account.balance;
            error -> 0
        end,
        NewBalance = case maps:find(K, NewAccounts) of
            {ok, NewAcc} -> NewAcc#account.balance;
            error -> 0
        end,
        case OldBalance =:= NewBalance of
            true -> ok;
            false -> io:format("  Account ~p: ~p -> ~p~n", [K, OldBalance, NewBalance])
        end
    end || K <- AllKeys].

%% Replica Process

replica_start(Id, Primary) ->
    DBPath = "db/replica_" ++ integer_to_list(Id) ++ "_db",
    % Add option to force close any existing DB lock when opening
    DBOpts = [
        {create_if_missing, true},
        {error_if_exists, false},
        {paranoid_checks, false}
    ],
    
    % Try to ensure directory exists
    filelib:ensure_dir(DBPath ++ "/"),
    
    % Attempt to open the DB with a retry mechanism
    open_db_with_retry(Id, DBPath, DBOpts, Primary, 3).
    
open_db_with_retry(Id, DBPath, DBOpts, Primary, Retries) ->
    case rocksdb:open(DBPath, DBOpts) of
        {ok, DB} ->
            replica_loop(Primary, DB, #state{db = DB});
        {error, Reason} ->
            if 
                Retries > 0 ->
                    % Wait and retry
                    error_logger:info_msg("Retry opening replica ~p DB. Retries left: ~p", [Id, Retries-1]),
                    timer:sleep(500),
                    open_db_with_retry(Id, DBPath, DBOpts, Primary, Retries-1);
                true ->
                    error_logger:error_msg("Replica ~p failed to open DB: ~p", [Id, Reason]),
                    exit({db_open_failed, Reason})
            end
    end.

replica_loop(Primary, DB, State = #state{accounts = Accounts, view = View}) ->
    receive
        {prepare_batch, RecvView, OpNumber, Batch, From} when RecvView =:= View ->
            case verify_checksum(Batch) of
                ok ->
                    ok = log_batch(DB, Batch, OpNumber),  % Store with OpNumber as key
                    From ! {prepare_ok, OpNumber, self()},
                    replica_loop(Primary, DB, State);
                {error, Reason} ->
                    error_logger:error_msg("Checksum mismatch in batch ~p: ~p", [Batch#batch.id, Reason]),
                    replica_loop(Primary, DB, State)
            end;

        {commit_batch, RecvView, OpNumber, Batch} when RecvView =:= View ->
            % FIX: Apply transfers ONLY ONCE, just like in the primary
            NewAccounts = apply_batch_once(Batch, Accounts),
            % Persist state after applying
            persist_state(DB, NewAccounts),
            replica_loop(Primary, DB, State#state{accounts = NewAccounts});

        {sync_state, NewPrimary, NewView, NewAccounts} ->
            persist_state(DB, NewAccounts),
            replica_loop(NewPrimary, DB, State#state{accounts = NewAccounts, view = NewView});

        {stop, _ReplyTo} ->
            rocksdb:close(DB),
            ok
    end.

%% Helper Functions

generate_batch_id() ->
    {Mega, Sec, Micro} = os:timestamp(),
    list_to_binary(integer_to_list(Mega * 1000000 + Sec) ++ "_" ++ integer_to_list(Micro)).

create_batch(BatchId, Transfers) ->
    Checksum = crypto:hash(sha256, term_to_binary(Transfers)),
    #batch{id = BatchId, transfers = Transfers, checksum = Checksum}.

log_batch(DB, Batch, OpNumber) ->
    % Store batch with operation number as key for consistent retrieval
    rocksdb:put(DB, term_to_binary({batch, OpNumber}), term_to_binary(Batch), [{sync, true}]).

get_batch(DB, OpNumber) ->
    case rocksdb:get(DB, term_to_binary({batch, OpNumber}), []) of
        {ok, Binary} -> binary_to_term(Binary);
        not_found -> error({batch_not_found, OpNumber})
    end.

persist_state(DB, Accounts) ->
    % Store the accounts state with a special key
    rocksdb:put(DB, <<"accounts_state">>, term_to_binary(Accounts), [{sync, true}]).

% Completely rewritten to only apply each transfer exactly ONCE
apply_batch_once(Batch, Accounts) ->
    % Debug info
    io:format("Applying batch with ~p transfers~n", [length(Batch#batch.transfers)]),
    
    % Apply each transfer sequentially and carefully
    lists:foldl(fun(Transfer, AccAccounts) -> 
        apply_single_transfer(Transfer, AccAccounts) 
    end, Accounts, Batch#batch.transfers).

apply_single_transfer(Transfer, Accounts) ->
    % Debug info
    io:format("Apply transfer: from=~p, to=~p, amount=~p~n", 
              [Transfer#transfer.from, Transfer#transfer.to, Transfer#transfer.amount]),
    
    % Get sender and receiver accounts, defaulting to new accounts if they don't exist
    FromId = Transfer#transfer.from,
    ToId = Transfer#transfer.to,
    Amount = Transfer#transfer.amount,
    
    From = case maps:find(FromId, Accounts) of
        {ok, FromAcc} -> FromAcc;
        error -> #account{id = FromId, balance = 0}
    end,
    
    To = case maps:find(ToId, Accounts) of
        {ok, ToAcc} -> ToAcc;
        error -> #account{id = ToId, balance = 0}
    end,
    
    % Debug: print current balances
    io:format("  Before: ~p balance=~p, ~p balance=~p~n", 
              [FromId, From#account.balance, ToId, To#account.balance]),
    
    % Calculate new balances (single application of the transfer)
    NewFromBalance = From#account.balance - Amount,
    NewToBalance = To#account.balance + Amount,
    
    % Debug: print new balances
    io:format("  After: ~p balance=~p, ~p balance=~p~n", 
              [FromId, NewFromBalance, ToId, NewToBalance]),
    
    % Create updated accounts with new balances and checksums
    NewFrom = From#account{
        balance = NewFromBalance,
        checksum = compute_checksum(FromId, NewFromBalance)
    },
    
    NewTo = To#account{
        balance = NewToBalance,
        checksum = compute_checksum(ToId, NewToBalance)
    },
    
    % Return updated accounts map
    Accounts#{
        FromId => NewFrom,
        ToId => NewTo
    }.

compute_checksum(AccountId, Balance) ->
    crypto:hash(sha256, term_to_binary({AccountId, Balance})).

verify_checksum(Batch) ->
    Expected = Batch#batch.checksum,
    Actual = crypto:hash(sha256, term_to_binary(Batch#batch.transfers)),
    case Expected =:= Actual of
        true -> ok;
        false -> {error, checksum_mismatch}
    end.

validate_transfers(Transfers, ProcessedIds) ->
    try
        ValidTransfers = [
            begin
                Id = element(1, Transfer),
                From = element(2, Transfer),
                To = element(3, Transfer),
                Amount = element(4, Transfer),
                
                case maps:is_key(Id, ProcessedIds) of
                    true -> throw({duplicate_transfer, Id});
                    false -> 
                        case Amount > 0 of
                            true ->
                                #transfer{
                                    id = Id, 
                                    from = From, 
                                    to = To, 
                                    amount = Amount, 
                                    timestamp = os:timestamp(), 
                                    checksum = crypto:hash(sha256, term_to_binary({Id, From, To, Amount}))
                                };
                            false ->
                                throw({invalid_amount, Amount})
                        end
                end
            end || Transfer <- Transfers
        ],
        {ok, ValidTransfers}
    catch
        throw:Reason ->
            {error, Reason}
    end.

update_processed_ids(Transfers, ProcessedIds) ->
    lists:foldl(fun(Transfer, Acc) -> 
        Id = element(1, Transfer),
        Acc#{Id => true} 
    end, ProcessedIds, Transfers).

quorum_size(Replicas) ->
    length(Replicas) div 2 + 1.

recover_state(State = #state{db = DB}) ->
    % First try to recover from the accounts_state key (new persistent format)
    case rocksdb:get(DB, <<"accounts_state">>, []) of
        {ok, Binary} ->
            Accounts = binary_to_term(Binary),
            error_logger:info_msg("Recovered accounts state with ~p accounts", [maps:size(Accounts)]),
            
            % Debug - print all accounts
            [error_logger:info_msg("Account ~p: Balance ~p", [Id, Acc#account.balance]) 
             || {Id, Acc} <- maps:to_list(Accounts)],
            
            State#state{accounts = Accounts};
        not_found ->
            % Fall back to older state format if needed
            case rocksdb:get(DB, <<"state">>, []) of
                {ok, OldBinary} ->
                    OldAccounts = binary_to_term(OldBinary),
                    error_logger:info_msg("Recovered old state format with ~p accounts", [maps:size(OldAccounts)]),
                    State#state{accounts = OldAccounts};
                not_found ->
                    error_logger:info_msg("No saved state found, starting fresh"),
                    State
            end
    end.

sync_replicas(State = #state{replicas = Replicas, view = View, accounts = Accounts}) ->
    [R ! {sync_state, self(), View, Accounts} || R <- Replicas].

monitor_primary(Primary, ReplicaCount) ->
    timer:sleep(1000), % Heartbeat interval
    case is_process_alive(Primary) of
        true -> monitor_primary(Primary, ReplicaCount);
        false ->
            NewView = get_current_view() + 1,
            NewPrimary = elect_new_primary(ReplicaCount),
            NewPrimary ! {view_change, NewView, NewPrimary},
            monitor_primary(NewPrimary, ReplicaCount)
    end.

get_current_view() ->
    % In a real system, this would be tracked persistently
    % For this implementation, we'll check the process dictionary
    case whereis(tigerbeetle) of
        undefined -> 1;
        Pid -> 
            try
                Pid ! {get_view, self()},
                receive
                    {view, View} -> View
                after 500 ->
                    1  % Default if no response
                end
            catch
                _:_ -> 1  % Default on any error
            end
    end.

elect_new_primary(ReplicaCount) ->
    % Simplified election: spawn a new primary
    spawn(fun() ->
        filelib:ensure_dir("db/primary_db/"),
        {ok, DB} = rocksdb:open("db/primary_db", [
            {create_if_missing, true},
            {error_if_exists, false},
            {paranoid_checks, false}
        ]),
        Replicas = [spawn(fun() -> replica_start(I, self()) end) || I <- lists:seq(1, ReplicaCount)],
        InitialState = #state{db = DB, replicas = Replicas},
        register(tigerbeetle, self()),
        loop(recover_state(InitialState))
    end).