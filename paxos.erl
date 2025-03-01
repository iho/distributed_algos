%%%-------------------------------------------------------------------
%%% @doc
%%% A basic implementation of the Paxos consensus algorithm in Erlang.
%%% This implementation includes three main roles:
%%% - Proposer: Proposes values to be chosen
%%% - Acceptor: Accepts or rejects proposals
%%% - Learner: Learns the chosen value
%%% @end
%%%-------------------------------------------------------------------

-module(paxos).
-export([start_proposer/3, start_acceptor/1, start_learner/2]).
-export([proposer/4, acceptor/2, learner/3]).

%% Message types:
%% {prepare, Proposer, Ballot}
%% {promise, Acceptor, Ballot, AcceptedBallot, AcceptedValue}
%% {accept, Proposer, Ballot, Value}
%% {accepted, Acceptor, Ballot, Value}
%% {learn, Acceptor, Ballot, Value}

%%====================================================================
%% API functions
%%====================================================================

% Start a proposer with a specific ballot number, value to propose, and list of acceptors
start_proposer(Ballot, Value, Acceptors) ->
    spawn(?MODULE, proposer, [Ballot, Value, Acceptors, []]).

% Start an acceptor with an initial state
start_acceptor(Name) ->
    spawn(?MODULE, acceptor, [Name, {0, none, none}]).

% Start a learner that needs to hear from QuorumSize acceptors
start_learner(Name, QuorumSize) ->
    spawn(?MODULE, learner, [Name, QuorumSize, #{}]).

%%====================================================================
%% Internal functions
%%====================================================================

%% Proposer Process
%% State: Ballot number, Value to propose, List of acceptors, Promises received
proposer(Ballot, Value, Acceptors, Promises) ->
    case length(Promises) >= length(Acceptors) div 2 + 1 of
        true ->
            % We have a quorum of promises, check if any has an accepted value
            % If so, we must use the value with the highest ballot number
            ProposalValue = case get_highest_accepted_value(Promises) of
                {_, none} -> Value; % No previously accepted value
                {_, V} -> V        % Use the previously accepted value
            end,
            
            % Send accept messages to all acceptors
            lists:foreach(
                fun(Acceptor) -> 
                    Acceptor ! {accept, self(), Ballot, ProposalValue} 
                end, 
                Acceptors
            ),
            
            % Wait for accepts
            proposer_wait_accepts(Ballot, ProposalValue, Acceptors, []);
        false ->
            % We don't have a quorum yet, send prepare messages
            lists:foreach(
                fun(Acceptor) -> 
                    Acceptor ! {prepare, self(), Ballot} 
                end, 
                Acceptors
            ),
            
            % Wait for promises
            receive
                {promise, Acceptor, B, AcceptedBallot, AcceptedValue} when B =:= Ballot ->
                    proposer(Ballot, Value, Acceptors, [{Acceptor, AcceptedBallot, AcceptedValue} | Promises]);
                {promise, _, B, _, _} when B > Ballot ->
                    % Someone has a higher ballot, retry with a higher one
                    proposer(B + 1, Value, Acceptors, [])
            after 1000 ->
                % Timeout, retry with a higher ballot
                proposer(Ballot + 10, Value, Acceptors, [])
            end
    end.

% Helper function to wait for accept responses
proposer_wait_accepts(Ballot, Value, Acceptors, Accepted) ->
    case length(Accepted) >= length(Acceptors) div 2 + 1 of
        true ->
            % We have a quorum of accepts, consensus reached!
            % Inform learners (in a real system, this could be a separate set of processes)
            io:format("Consensus reached for value: ~p with ballot: ~p~n", [Value, Ballot]);
        false ->
            receive
                {accepted, Acceptor, B, V} when B =:= Ballot, V =:= Value ->
                    proposer_wait_accepts(Ballot, Value, Acceptors, [Acceptor | Accepted]);
                {accepted, _, B, _} when B > Ballot ->
                    % Someone has a higher ballot, retry with a higher one
                    proposer(B + 1, Value, Acceptors, [])
            after 1000 ->
                % Timeout, retry with a higher ballot
                proposer(Ballot + 10, Value, Acceptors, [])
            end
    end.

% Helper function to get highest accepted value from promises
get_highest_accepted_value(Promises) ->
    lists:foldl(
        fun({_, AcceptedBallot, AcceptedValue}, {MaxBallot, MaxValue}) ->
            case AcceptedBallot > MaxBallot of
                true -> {AcceptedBallot, AcceptedValue};
                false -> {MaxBallot, MaxValue}
            end
        end,
        {0, none},
        Promises
    ).

%% Acceptor Process
%% State: {PromisedBallot, AcceptedBallot, AcceptedValue}
acceptor(Name, {PromisedBallot, AcceptedBallot, AcceptedValue} = State) ->
    receive
        {prepare, Proposer, Ballot} ->
            case Ballot > PromisedBallot of
                true ->
                    % Promise to not accept any lower ballot proposals
                    Proposer ! {promise, self(), Ballot, AcceptedBallot, AcceptedValue},
                    acceptor(Name, {Ballot, AcceptedBallot, AcceptedValue});
                false ->
                    % Reject this prepare request (by ignoring or explicitly rejecting)
                    acceptor(Name, State)
            end;
        
        {accept, Proposer, Ballot, Value} ->
            case Ballot >= PromisedBallot of
                true ->
                    % Accept this proposal
                    io:format("Acceptor ~p accepting ballot ~p with value ~p~n", [Name, Ballot, Value]),
                    Proposer ! {accepted, self(), Ballot, Value},
                    
                    % Also inform all learners (in a real system, this would be a separate list)
                    % Send a learn message to learners
                    acceptor(Name, {PromisedBallot, Ballot, Value});
                false ->
                    % Reject this accept request (by ignoring or explicitly rejecting)
                    acceptor(Name, State)
            end
    end.

%% Learner Process
%% State: QuorumSize and a map of {Ballot, Value} -> [Acceptors]
learner(Name, QuorumSize, Learned) ->
    receive
        {learn, Acceptor, Ballot, Value} ->
            % Update our tracking of who has accepted what
            Key = {Ballot, Value},
            NewLearned = case maps:find(Key, Learned) of
                {ok, Acceptors} -> 
                    maps:put(Key, [Acceptor | Acceptors], Learned);
                error -> 
                    maps:put(Key, [Acceptor], Learned)
            end,
            
            % Check if we have a quorum for any ballot/value pair
            case maps:fold(
                fun(K, Acceptors, none) ->
                    case length(Acceptors) >= QuorumSize of
                        true -> {K, Acceptors};
                        false -> none
                    end;
                   (_, _, Result) -> Result
                end,
                none,
                NewLearned
            ) of
                none ->
                    % No quorum yet
                    learner(Name, QuorumSize, NewLearned);
                {{ConsensusB, ConsensusV}, _} ->
                    % We have a quorum!
                    io:format("Learner ~p learned consensus: ~p (ballot ~p)~n", 
                              [Name, ConsensusV, ConsensusB]),
                    learner(Name, QuorumSize, NewLearned)
            end
    end.

%%====================================================================
%% Example usage
%%====================================================================

%% In the shell, you can test this with:
%%
%% 1> A1 = paxos:start_acceptor(a1).
%% 2> A2 = paxos:start_acceptor(a2).
%% 3> A3 = paxos:start_acceptor(a3).
%% 4> Acceptors = [A1, A2, A3].
%% 5> L1 = paxos:start_learner(l1, 2).
%% 6> P1 = paxos:start_proposer(1, "hello world", Acceptors).
%%
%% You can then start another proposer with a higher ballot number to see how conflicts are resolved:
%% 7> P2 = paxos:start_proposer(2, "goodbye world", Acceptors).