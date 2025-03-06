-module(tigerbeetle).
-export([start/0, create_transfer/3, get_balance/1, stop/0]).

% Constants from ARCHITECTURE.md
-define(BLOCK_SIZE, 4096).              % 4KiB blocks
-define(SUPERBLOCK_COPIES, 2).          % Two superblock copies
-define(ALIGNMENT, 512).               % Direct I/O alignment
-define(MAX_TRANSFERS_PER_COMMIT, 8192).

% Record definitions
-record(superblock, {
    version = 1 :: integer(),
    checksum :: integer(),
    commit = 0 :: integer(),           % Highest committed op
    trailer = <<>> :: binary()         % Trailer for recovery
}).

-record(account, {
    id :: integer(),
    balance = 0 :: integer(),
    checksum :: integer()
}).

-record(transfer, {
    id :: integer(),
    from :: integer(),
    to :: integer(),
    amount :: integer(),
    timestamp :: integer(),
    checksum :: integer()
}).

-record(grid_block, {
    offset :: integer(),
    data :: binary(),
    checksum :: integer()
}).

-record(state, {
    superblock :: #superblock{},
    wal :: file:io_device(),           % Write-ahead log
    grid :: file:io_device(),          % Main storage grid
    accounts :: map(),
    transfers :: map(),
    pending :: queue:queue(),          % Pending operations
    commit_max = 0 :: integer(),       % Highest committed op
    replicas = [] :: [pid()]           % Replica PIDs
}).

% Start the server
start() ->
    % Open files with Direct I/O flags (simulated)
    {ok, WAL} = file:open("tigerbeetle.wal", [read, write, raw, binary, {delayed_write, ?BLOCK_SIZE, 100}]),
    {ok, Grid} = file:open("tigerbeetle.grid", [read, write, raw, binary]),
    
    % Initialize superblock
    Superblock = #superblock{
        version = 1,
        checksum = 0,
        commit = 0,
        trailer = create_trailer(0)
    },
    
    InitialState = #state{
        superblock = Superblock,
        wal = WAL,
        grid = Grid,
        accounts = #{},
        transfers = #{},
        pending = queue:new(),
        replicas = spawn_replicas(2)  % Two replicas for VR
    },
    
    % Recover state if exists
    State = recover_state(InitialState),
    Pid = spawn(fun() -> loop(State) end),
    register(tigerbeetle, Pid),
    {ok, Pid}.

% Main server loop
loop(State) ->
    receive
        {create_transfer, From, To, Amount, ReplyTo} ->
            NewState = queue_transfer(From, To, Amount, ReplyTo, State),
            loop(NewState);
        {get_balance, AccountId, ReplyTo} ->
            Balance = get_account_balance(AccountId, State),
            ReplyTo ! {ok, Balance},
            loop(State);
        {commit, OpNumber} ->
            NewState = commit_operations(OpNumber, State),
            loop(NewState);
        {replicate, Op, FromReplica} ->
            NewState = handle_replication(Op, FromReplica, State),
            loop(NewState);
        {stop, ReplyTo} ->
            FinalState = shutdown(State),
            ReplyTo ! ok,
            ok
    end.

% Client API
create_transfer(From, To, Amount) ->
    tigerbeetle ! {create_transfer, From, To, Amount, self()},
    receive
        {ok, _} -> ok
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
queue_transfer(From, To, Amount, ReplyTo, State = #state{pending = Pending}) ->
    OpNumber = State#state.commit_max + queue:len(Pending) + 1,
    Transfer = #transfer{
        id = OpNumber,
        from = From,
        to = To,
        amount = Amount,
        timestamp = erlang:system_time(second),
        checksum = 0
    },
    NewPending = queue:in({Transfer, ReplyTo}, Pending),
    % Write to WAL
    write_wal_entry(State#state.wal, Transfer, OpNumber),
    % Replicate to others
    replicate_to_peers(State#state.replicas, {transfer, Transfer, OpNumber}),
    State#state{pending = NewPending}.

% Commit operations up to OpNumber
commit_operations(OpNumber, State = #state{pending = Pending, accounts = Accounts, transfers = Transfers}) ->
    case OpNumber =< State#state.commit_max of
        true -> State;  % Already committed
        false ->
            {ToCommit, Remaining} = split_queue(OpNumber - State#state.commit_max, Pending),
            NewState = lists:foldl(fun({Transfer, ReplyTo}, Acc) ->
                Acc1 = apply_transfer(Transfer, Acc),
                ReplyTo ! {ok, Transfer#transfer.id},
                Acc1
            end, State, ToCommit),
            % Update superblock
            NewSuperblock = update_superblock(NewState#state.superblock, OpNumber),
            write_superblock(NewState#state.grid, NewSuperblock),
            NewState#state{
                superblock = NewSuperblock,
                pending = Remaining,
                commit_max = OpNumber,
                accounts = update_grid(NewState#state.grid, NewState#state.accounts),
                transfers = Transfers#{OpNumber => hd(ToCommit)}
            }
    end.

% Apply transfer to state
apply_transfer(Transfer, State = #state{accounts = Accounts}) ->
    FromAccount = maps:get(Transfer#transfer.from, Accounts, #account{id = Transfer#transfer.from}),
    ToAccount = maps:get(Transfer#transfer.to, Accounts, #account{id = Transfer#transfer.to}),
    case FromAccount#account.balance >= Transfer#transfer.amount of
        true ->
            NewFrom = FromAccount#account{
                balance = FromAccount#account.balance - Transfer#transfer.amount,
                checksum = calculate_checksum(FromAccount)
            },
            NewTo = ToAccount#account{
                balance = ToAccount#account.balance + Transfer#transfer.amount,
                checksum = calculate_checksum(ToAccount)
            },
            State#state{accounts = Accounts#{Transfer#transfer.from => NewFrom, Transfer#transfer.to => NewTo}};
        false ->
            State  % Insufficient funds
    end.

% Write-ahead log entry
write_wal_entry(WAL, Transfer, OpNumber) ->
    Data = term_to_binary({OpNumber, Transfer}),
    Padding = (?ALIGNMENT - (byte_size(Data) rem ?ALIGNMENT)) rem ?ALIGNMENT,
    PaddedData = <<Data/binary, 0:(Padding*8)>>,
    file:pwrite(WAL, OpNumber * ?BLOCK_SIZE, PaddedData),
    file:sync(WAL).

% Superblock management
update_superblock(Superblock, Commit) ->
    NewTrailer = create_trailer(Commit),
    Data = term_to_binary({Superblock#superblock.version, Commit}),
    Checksum = erlang:crc32(<<Data/binary, NewTrailer/binary>>),
    Superblock#superblock{commit = Commit, checksum = Checksum, trailer = NewTrailer}.

write_superblock(Grid, Superblock) ->
    Data = term_to_binary(Superblock),
    Padding = (?BLOCK_SIZE - (byte_size(Data) rem ?BLOCK_SIZE)) rem ?BLOCK_SIZE,
    PaddedData = <<Data/binary, 0:(Padding*8)>>,
    [file:pwrite(Grid, Idx * ?BLOCK_SIZE * 1024, PaddedData) || Idx <- lists:seq(0, ?SUPERBLOCK_COPIES-1)],
    file:sync(Grid).

create_trailer(Commit) ->
    crypto:strong_rand_bytes(128).  % 128-byte trailer

% Grid management
update_grid(Grid, Accounts) ->
    Block = #grid_block{
        offset = maps:size(Accounts) * ?BLOCK_SIZE,
        data = term_to_binary(Accounts),
        checksum = erlang:crc32(term_to_binary(Accounts))
    },
    write_grid_block(Grid, Block),
    Accounts.

write_grid_block(Grid, Block) ->
    Data = term_to_binary(Block),
    Padding = (?BLOCK_SIZE - (byte_size(Data) rem ?BLOCK_SIZE)) rem ?BLOCK_SIZE,
    PaddedData = <<Data/binary, 0:(Padding*8)>>,
    file:pwrite(Grid, Block#grid_block.offset, PaddedData).

% Replication (simplified Viewstamped Replication)
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

handle_replication({Transfer, OpNumber}, _From, State) ->
    write_wal_entry(State#state.wal, Transfer, OpNumber),
    State.

% Recovery
recover_state(State) ->
    % Simplified: read latest superblock and WAL
    {ok, SuperblockData} = file:pread(State#state.grid, 0, ?BLOCK_SIZE),
    Superblock = binary_to_term(SuperblockData),
    State#state{superblock = Superblock, commit_max = Superblock#superblock.commit}.

% Helpers
get_account_balance(AccountId, #state{accounts = Accounts}) ->
    case maps:get(AccountId, Accounts, undefined) of
        undefined -> 0;
        Account -> Account#account.balance
    end.

calculate_checksum(Record) ->
    erlang:crc32(term_to_binary(Record)).

split_queue(N, Queue) ->
    {queue:take(N, Queue), queue:drop(N, Queue)}.

shutdown(State) ->
    write_superblock(State#state.grid, State#state.superblock),
    file:close(State#state.wal),
    file:close(State#state.grid),
    State#state{wal = undefined, grid = undefined}.