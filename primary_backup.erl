%%%-------------------------------------------------------------------
%%% @doc Primary-Backup Replication System
%%% 
%%% This module implements a simple primary-backup replication system in Erlang
%%% using basic processes and message passing (without gen_server).
%%% It provides functionality for:
%%%   - Starting primary and backup nodes
%%%   - Handling operations on the primary with replication to backups
%%%   - Failure detection and primary election when primary fails
%%%-------------------------------------------------------------------

-module(primary_backup).

%% API
-export([
    start_primary/1,
    start_backup/2, 
    stop/1,
    get/2,
    put/3,
    delete/2
]).

%% Process loop functions
-export([
    primary_loop/2,
    backup_loop/3
]).

-define(HEARTBEAT_INTERVAL, 1000).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Start a primary node with a given name
start_primary(NodeName) ->
    Pid = spawn(fun() -> primary_init(NodeName) end),
    register(NodeName, Pid),
    {ok, Pid}.

%% @doc Start a backup node with a given name that connects to primary
start_backup(NodeName, PrimaryPid) ->
    Pid = spawn(fun() -> backup_init(NodeName, PrimaryPid) end),
    register(NodeName, Pid),
    {ok, Pid}.

%% @doc Stop a node
stop(Pid) ->
    Pid ! {stop, self()},
    receive
        {stopped, Pid} -> ok
    after 
        5000 -> {error, timeout}
    end.

%% @doc Get a value from the primary
get(PrimaryPid, Key) ->
    PrimaryPid ! {get, Key, self()},
    receive
        {get_response, Response} -> Response
    after 
        5000 -> {error, timeout}
    end.

%% @doc Put a value to the primary (will be replicated to backups)
put(PrimaryPid, Key, Value) ->
    PrimaryPid ! {put, Key, Value, self()},
    receive
        {put_response, Response} -> Response
    after 
        5000 -> {error, timeout}
    end.

%% @doc Delete a key from the primary (will be replicated to backups)
delete(PrimaryPid, Key) ->
    PrimaryPid ! {delete, Key, self()},
    receive
        {delete_response, Response} -> Response
    after 
        5000 -> {error, timeout}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Primary initialization
primary_init(NodeName) ->
    process_flag(trap_exit, true),
    timer:send_interval(?HEARTBEAT_INTERVAL, send_heartbeat),
    primary_loop(NodeName, #{}).

%% Backup initialization
backup_init(NodeName, PrimaryPid) ->
    process_flag(trap_exit, true),
    link(PrimaryPid),
    PrimaryPid ! {register_backup, self()},
    timer:send_interval(?HEARTBEAT_INTERVAL * 2, check_primary_health),
    backup_loop(NodeName, PrimaryPid, #{}).

%% Primary node main loop
primary_loop(NodeName, State) ->
    BackupNodes = maps:get(backup_nodes, State, []),
    Data = maps:get(data, State, #{}),
    
    receive
        % Handle client requests
        {get, Key, From} ->
            Response = case maps:find(Key, Data) of
                {ok, Value} -> {ok, Value};
                error -> {error, not_found}
            end,
            From ! {get_response, Response},
            primary_loop(NodeName, State);
            
        {put, Key, Value, From} ->
            % Update local data
            NewData = maps:put(Key, Value, Data),
            
            % Replicate to all backup nodes
            [Backup ! {replicate, {put, Key, Value}} || Backup <- BackupNodes],
            
            From ! {put_response, ok},
            primary_loop(NodeName, State#{data => NewData});
            
        {delete, Key, From} ->
            % Update local data
            NewData = maps:remove(Key, Data),
            
            % Replicate to all backup nodes
            [Backup ! {replicate, {delete, Key}} || Backup <- BackupNodes],
            
            From ! {delete_response, ok},
            primary_loop(NodeName, State#{data => NewData});
            
        % Handle backup registration
        {register_backup, BackupPid} ->
            link(BackupPid),
            % Send current data to the new backup
            BackupPid ! {init_data, Data},
            primary_loop(NodeName, State#{backup_nodes => [BackupPid | BackupNodes]});
            
        % Handle heartbeats to backups
        send_heartbeat ->
            [Backup ! {heartbeat, self()} || Backup <- BackupNodes],
            primary_loop(NodeName, State);
            
        % Handle backup node failure
        {'EXIT', Pid, Reason} ->
            io:format("Backup node ~p exited with reason: ~p~n", [Pid, Reason]),
            NewBackups = lists:delete(Pid, BackupNodes),
            primary_loop(NodeName, State#{backup_nodes => NewBackups});
            
        % Handle shutdown request
        {stop, From} ->
            From ! {stopped, self()},
            exit(normal);
            
        % Unknown messages
        Unknown ->
            io:format("Primary received unknown message: ~p~n", [Unknown]),
            primary_loop(NodeName, State)
    end.

%% Backup node main loop
backup_loop(NodeName, PrimaryPid, State) ->
    Data = maps:get(data, State, #{}),
    LastHeartbeat = maps:get(last_heartbeat, State, erlang:system_time(millisecond)),
    
    receive
        % Handle replication messages from primary
        {replicate, {put, Key, Value}} ->
            NewData = maps:put(Key, Value, Data),
            backup_loop(NodeName, PrimaryPid, State#{data => NewData});
            
        {replicate, {delete, Key}} ->
            NewData = maps:remove(Key, Data),
            backup_loop(NodeName, PrimaryPid, State#{data => NewData});
            
        % Handle initial data loading
        {init_data, NewData} ->
            backup_loop(NodeName, PrimaryPid, State#{data => NewData});
            
        % Handle heartbeat from primary
        {heartbeat, PrimaryPid} ->
            Now = erlang:system_time(millisecond),
            backup_loop(NodeName, PrimaryPid, State#{last_heartbeat => Now});
            
        % Check if primary is still alive
        check_primary_health ->
            Now = erlang:system_time(millisecond),
            TimeSinceLastHeartbeat = Now - LastHeartbeat,
            
            if
                TimeSinceLastHeartbeat > ?HEARTBEAT_INTERVAL * 3 ->
                    io:format("Primary node appears to be down. Starting election...~n"),
                    % Basic implementation - this backup becomes primary
                    % In a real system, you'd implement a proper election algorithm
                    become_primary(NodeName, Data);
                true ->
                    backup_loop(NodeName, PrimaryPid, State)
            end;
            
        % Handle primary node failure
        {'EXIT', PrimaryPid, Reason} ->
            io:format("Primary node exited with reason: ~p. Starting election...~n", [Reason]),
            % Basic implementation - this backup becomes primary
            become_primary(NodeName, Data);
            
        % Client requests - redirect to primary
        {get, Key, From} ->
            PrimaryPid ! {get, Key, From},
            backup_loop(NodeName, PrimaryPid, State);
            
        {put, Key, Value, From} ->
            PrimaryPid ! {put, Key, Value, From},
            backup_loop(NodeName, PrimaryPid, State);
            
        {delete, Key, From} ->
            PrimaryPid ! {delete, Key, From},
            backup_loop(NodeName, PrimaryPid, State);
            
        % Handle shutdown request
        {stop, From} ->
            From ! {stopped, self()},
            exit(normal);
            
        % Unknown messages
        Unknown ->
            io:format("Backup received unknown message: ~p~n", [Unknown]),
            backup_loop(NodeName, PrimaryPid, State)
    end.

%% Promotion of backup to primary
become_primary(NodeName, Data) ->
    io:format("Node ~p is becoming the new primary~n", [NodeName]),
    timer:send_interval(?HEARTBEAT_INTERVAL, send_heartbeat),
    primary_loop(NodeName, #{data => Data, backup_nodes => []}).