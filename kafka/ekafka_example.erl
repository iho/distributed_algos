%%%-------------------------------------------------------------------
%%% @doc Simple example application using the ekafka module
%%%-------------------------------------------------------------------
-module(ekafka_example).

-export([
    start/0,
    run_producer/0,
    run_consumer/0
]).

-record(message, {
    key,
    value,
    timestamp,
    offset,
    headers = []
}).

%% Main entry point
start() ->
    % Ensure dependencies are started
    ok = application:start(rocksdb),
    ok = application:start(ra),
    
    % Start ekafka
    Config = #{
        data_dir => "ekafka_data",
        node_id => node()
    },
    {ok, _} = ekafka:start(Config),
    
    % Setup example topic if it doesn't exist
    case lists:member("example_topic", ekafka:list_topics()) of
        false ->
            TopicConfig = #{
                partitions => 3,
                replication_factor => 1
            },
            {ok, _} = ekafka:create_topic("example_topic", TopicConfig);
        true ->
            io:format("Topic already exists~n")
    end,
    
    % Start producer and consumer processes
    spawn(fun() -> run_producer() end),
    spawn(fun() -> run_consumer() end),
    
    ok.

%% Producer process
run_producer() ->
    % Produce a message every second
    Topic = "example_topic",
    Partition = 0,
    
    % Generate a message
    Timestamp = erlang:system_time(millisecond),
    Message = #{
        key => <<"key-", (integer_to_binary(Timestamp rem 100))/binary>>,
        value => <<"Message produced at ", (integer_to_binary(Timestamp))/binary>>,
        headers => [
            {<<"producer">>, <<"example_producer">>}
        ]
    },
    
    % Produce the message
    case ekafka:produce(Topic, Partition, Message) of
        {ok, Offset} ->
            io:format("[Producer] Message sent with offset: ~p~n", [Offset]);
        Error ->
            io:format("[Producer] Failed to send message: ~p~n", [Error])
    end,
    
    % Sleep for a second
    timer:sleep(1000),
    
    % Loop
    run_producer().

%% Consumer process
run_consumer() ->
    % Consume messages every 2 seconds
    Topic = "example_topic",
    Partition = 0,
    
    % Consume up to 5 messages
    case ekafka:consume_batch(Topic, Partition, 5) of
        {ok, []} ->
            io:format("[Consumer] No messages available~n");
        {ok, Messages} ->
            io:format("[Consumer] Received ~p messages~n", [length(Messages)]),
            [process_message(Msg) || Msg <- Messages];
        Error ->
            io:format("[Consumer] Error consuming messages: ~p~n", [Error])
    end,
    
    % Sleep for two seconds
    timer:sleep(2000),
    
    % Loop
    run_consumer().

%% Process a single message
process_message(Msg) ->
    % Extract data from message record
    #message{
        key = Key,
        value = Value,
        timestamp = Timestamp,
        offset = Offset,
        headers = Headers
    } = Msg,
    
    % Print message details
    io:format("[Consumer] Processing message:~n"),
    io:format("  Offset: ~p~n", [Offset]),
    io:format("  Key: ~p~n", [Key]),
    io:format("  Value: ~p~n", [Value]),
    io:format("  Timestamp: ~p~n", [Timestamp]),
    io:format("  Headers: ~p~n", [Headers]),
    
    % Do something with the message (just print in this example)
    ok.