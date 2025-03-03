%%%-------------------------------------------------------------------
%%% @doc Game of Life in Erlang using actors without gen_server
%%% Each actor processes a 100x100 cell block within a 1000x1000 grid
%%%-------------------------------------------------------------------

-module(game_of_life).
-export([start/0, start/1, stop/0, print_state/0]).
-export([block_loop/4, controller_loop/3, print_grid/1]).

% Grid dimensions
-define(TOTAL_WIDTH, 1000).
-define(TOTAL_HEIGHT, 1000).
-define(BLOCK_SIZE, 100).
-define(BLOCKS_PER_ROW, (?TOTAL_WIDTH div ?BLOCK_SIZE)).
-define(BLOCKS_PER_COL, (?TOTAL_HEIGHT div ?BLOCK_SIZE)).

% Default number of iterations
-define(ITERATIONS, 100).

%%% Public API

%% @doc Start the game with default iterations
start() ->
    start(?ITERATIONS).

%% @doc Start the game with custom iterations
start(Iterations) when is_integer(Iterations) ->
    io:format("Starting Game of Life (~px~p) for ~p iterations~n", 
              [?TOTAL_WIDTH, ?TOTAL_HEIGHT, Iterations]),
    
    % Register controller process
    Controller = spawn(fun() -> init_controller(Iterations) end),
    register(game_controller, Controller),
    {ok, Controller}.

%% @doc Stop the game
stop() ->
    case whereis(game_controller) of
        undefined ->
            {error, not_running};
        Pid ->
            Pid ! stop,
            unregister(game_controller),
            {ok, stopped}
    end.

%% @doc Print the current state of the game to the terminal
print_state() ->
    case whereis(game_controller) of
        undefined ->
            {error, not_running};
        Pid ->
            Pid ! {manual_print},
            {ok, printing}
    end.

%%% Internal functions

%% @doc Initialize the controller
init_controller(Iterations) ->
    % Create a grid of block processes
    Blocks = init_blocks(),
    % Start the simulation
    controller_loop(Blocks, maps:size(Blocks), Iterations).

%% @doc Initialize blocks and create block processes
init_blocks() ->
    % Create processes for blocks
    Blocks = lists:foldl(
        fun(BlockY, AccY) ->
            lists:foldl(
                fun(BlockX, AccX) ->
                    % Initialize a block with a random grid
                    Grid = init_random_grid(),
                    BlockId = {BlockX, BlockY},
                    Pid = spawn(?MODULE, block_loop, [BlockId, Grid, #{}, self()]),
                    maps:put(BlockId, Pid, AccX)
                end,
                AccY,
                lists:seq(0, ?BLOCKS_PER_ROW - 1)
            )
        end,
        #{},
        lists:seq(0, ?BLOCKS_PER_COL - 1)
    ),
    
    % Set up the neighbor blocks for each block
    maps:map(
        fun(BlockId, Pid) ->
            NeighborBlocks = get_neighbor_blocks(BlockId, Blocks),
            Pid ! {set_neighbors, NeighborBlocks},
            Pid
        end,
        Blocks
    ),
    
    Blocks.

%% @doc Initialize a random grid for a block (100x100)
init_random_grid() ->
    % Create a 100x100 grid with random cell states
    lists:foldl(
        fun(Y, AccY) ->
            lists:foldl(
                fun(X, AccX) ->
                    % Randomly initialize cells (about 25% alive)
                    Alive = (rand:uniform(100) =< 25),
                    % Store cell state in a map with coordinates as key
                    maps:put({X, Y}, Alive, AccX)
                end,
                AccY,
                lists:seq(0, ?BLOCK_SIZE - 1)
            )
        end,
        #{},
        lists:seq(0, ?BLOCK_SIZE - 1)
    ).

%% @doc Get the neighbor blocks of a block at the given coordinates
get_neighbor_blocks({BlockX, BlockY}, Blocks) ->
    % Define offsets to find the 8 surrounding blocks plus the block itself
    Offsets = [
        {-1, -1}, {0, -1}, {1, -1},
        {-1,  0}, {0,  0}, {1,  0},
        {-1,  1}, {0,  1}, {1,  1}
    ],
    
    lists:filtermap(
        fun({DX, DY}) ->
            NX = (BlockX + DX + ?BLOCKS_PER_ROW) rem ?BLOCKS_PER_ROW,
            NY = (BlockY + DY + ?BLOCKS_PER_COL) rem ?BLOCKS_PER_COL,
            
            case maps:find({NX, NY}, Blocks) of
                {ok, NeighborPid} -> {true, {{DX, DY}, NeighborPid}};
                error -> false
            end
        end,
        Offsets
    ).

%% @doc Controller loop that coordinates the simulation
controller_loop(Blocks, BlockCount, RemainingIterations) ->
    receive
        stop ->
            % Send stop message to all blocks
            maps:map(fun(_, Pid) -> Pid ! stop end, Blocks),
            io:format("Game stopped~n");
        
        {manual_print} ->
            % Print the current state on demand
            io:format("Printing current state...~n"),
            print_grid(Blocks),
            controller_loop(Blocks, BlockCount, RemainingIterations);
        
        {iteration_complete, _BlockId} ->
            % Keep track of completed blocks
            UpdatedBlocksComplete = get(blocks_complete) + 1,
            put(blocks_complete, UpdatedBlocksComplete),
            
            % If all blocks completed, move to next iteration
            case UpdatedBlocksComplete of
                BlockCount ->
                    put(blocks_complete, 0),
                    
                    % Print the current state to terminal
                    CurrentIteration = ?ITERATIONS - RemainingIterations + 1,
                    io:format("Iteration ~p completed~n", [CurrentIteration]),
                    print_grid(Blocks),
                    
                    % Add 1-second delay between iterations
                    timer:sleep(1000),
                    
                    case RemainingIterations of
                        1 ->
                            io:format("Finished all iterations~n"),
                            maps:map(fun(_, Pid) -> Pid ! stop end, Blocks);
                        _ ->
                            % Start the next iteration
                            maps:map(fun(_, Pid) -> Pid ! next_iteration end, Blocks),
                            controller_loop(Blocks, BlockCount, RemainingIterations - 1)
                    end;
                _ ->
                    % Wait for more blocks to complete
                    controller_loop(Blocks, BlockCount, RemainingIterations)
            end;
            
        {get_state, From} ->
            % Request state from all blocks
            maps:map(fun(BlockId, Pid) -> Pid ! {get_state, From, BlockId} end, Blocks),
            controller_loop(Blocks, BlockCount, RemainingIterations)
    end.

%% @doc Block process loop - handles a 100x100 area
block_loop(BlockId, Grid, Neighbors, Controller) ->
    receive
        {set_neighbors, NeighborList} ->
            block_loop(BlockId, Grid, maps:from_list(NeighborList), Controller);
            
        next_iteration ->
            % Step 1: Exchange border information with neighbors
            % We need cell states from neighboring blocks to compute our new state
            exchange_borders(BlockId, Grid, Neighbors),
            
            % Step 2: Wait to receive all border data from neighbors
            BorderData = receive_borders(maps:size(Neighbors), #{}),
            
            % Step 3: Compute the next generation using our grid and the border data
            NextGrid = compute_next_generation(Grid, BorderData),
            
            % Step 4: Notify controller that iteration is complete
            Controller ! {iteration_complete, BlockId},
            
            % Continue with the updated grid
            block_loop(BlockId, NextGrid, Neighbors, Controller);
            
        {get_border, NeighborId, FromPid} ->
            % Extract and send border cells requested by a neighbor
            Border = extract_border(NeighborId, Grid),
            FromPid ! {border_data, {BlockId, NeighborId}, Border},
            block_loop(BlockId, Grid, Neighbors, Controller);
            
        {border_data, _, _} = BorderMsg ->
            % Store border data for later processing
            put(border_buffer, [BorderMsg | get(border_buffer)]),
            block_loop(BlockId, Grid, Neighbors, Controller);
            
        {get_state, From, Id} ->
            % Return current state to caller
            From ! {block_state, Id, Grid},
            block_loop(BlockId, Grid, Neighbors, Controller);
            
        stop ->
            % Stop the process
            ok
    end.

%% @doc Exchange border information with neighboring blocks
exchange_borders(BlockId, Grid, Neighbors) ->
    % Initialize border buffer
    put(border_buffer, []),
    
    % Request borders from all neighbors
    maps:map(
        fun(RelativePos, Pid) ->
            Pid ! {get_border, invert_position(RelativePos), self()}
        end,
        Neighbors
    ).

%% @doc Invert a relative position
invert_position({X, Y}) ->
    {-X, -Y}.

%% @doc Receive borders from all neighbors
receive_borders(0, Acc) ->
    % All borders received
    Acc;
receive_borders(Remaining, Acc) ->
    case get(border_buffer) of
        [] ->
            % Wait for more borders
            timer:sleep(1),
            receive_borders(Remaining, Acc);
        [H|T] ->
            % Process next border
            put(border_buffer, T),
            {border_data, {FromId, ToId}, Border} = H,
            receive_borders(Remaining - 1, maps:put({FromId, ToId}, Border, Acc))
    end.

%% @doc Extract border cells to send to a neighbor
extract_border({DX, DY}, Grid) ->
    % Determine which cells to extract based on relative position
    case {DX, DY} of
        % Corner cases
        {-1, -1} -> extract_corner(Grid, 0, 0);                         % Top-left
        {1, -1} -> extract_corner(Grid, ?BLOCK_SIZE - 1, 0);            % Top-right
        {-1, 1} -> extract_corner(Grid, 0, ?BLOCK_SIZE - 1);            % Bottom-left
        {1, 1} -> extract_corner(Grid, ?BLOCK_SIZE - 1, ?BLOCK_SIZE - 1); % Bottom-right
        
        % Edge cases
        {0, -1} -> extract_row(Grid, 0);                                % Top
        {0, 1} -> extract_row(Grid, ?BLOCK_SIZE - 1);                   % Bottom
        {-1, 0} -> extract_column(Grid, 0);                             % Left
        {1, 0} -> extract_column(Grid, ?BLOCK_SIZE - 1);                % Right
        
        % Self - should not happen, but handle it anyway
        {0, 0} -> #{}
    end.

%% @doc Extract a corner cell and its adjacent cells
extract_corner(Grid, X, Y) ->
    % Extract a 3x3 area around the corner
    lists:foldl(
        fun(DY, AccY) ->
            lists:foldl(
                fun(DX, AccX) ->
                    NX = X + DX,
                    NY = Y + DY,
                    % Only include cells that are in this block
                    case is_in_block(NX, NY) of
                        true ->
                            State = maps:get({NX, NY}, Grid, false),
                            maps:put({NX, NY}, State, AccX);
                        false ->
                            AccX
                    end
                end,
                AccY,
                [-1, 0, 1]
            )
        end,
        #{},
        [-1, 0, 1]
    ).

%% @doc Extract a row of cells
extract_row(Grid, Y) ->
    lists:foldl(
        fun(X, Acc) ->
            State = maps:get({X, Y}, Grid, false),
            maps:put({X, Y}, State, Acc)
        end,
        #{},
        lists:seq(0, ?BLOCK_SIZE - 1)
    ).

%% @doc Extract a column of cells
extract_column(Grid, X) ->
    lists:foldl(
        fun(Y, Acc) ->
            State = maps:get({X, Y}, Grid, false),
            maps:put({X, Y}, State, Acc)
        end,
        #{},
        lists:seq(0, ?BLOCK_SIZE - 1)
    ).

%% @doc Check if a cell is within the block
is_in_block(X, Y) ->
    X >= 0 andalso X < ?BLOCK_SIZE andalso
    Y >= 0 andalso Y < ?BLOCK_SIZE.

%% @doc Compute the next generation using the block's grid and border data
compute_next_generation(Grid, BorderData) ->
    % Merge all border data into a single expanded grid
    ExpandedGrid = maps:fold(
        fun({_, _}, Border, Acc) ->
            maps:merge(Acc, Border)
        end,
        Grid,
        BorderData
    ),
    
    % Compute new state for each cell in our block
    maps:fold(
        fun({X, Y}, _, Acc) ->
            % Only compute cells that are actually in our block
            case is_in_block(X, Y) of
                true ->
                    % Count live neighbors
                    LiveCount = count_live_neighbors(X, Y, ExpandedGrid),
                    CurrentState = maps:get({X, Y}, ExpandedGrid, false),
                    
                    % Apply Game of Life rules
                    NextState = case {CurrentState, LiveCount} of
                        {true, N} when N < 2 -> false;    % Underpopulation
                        {true, N} when N =< 3 -> true;    % Survival
                        {true, _} -> false;               % Overpopulation
                        {false, 3} -> true;               % Reproduction
                        {false, _} -> false               % Stay dead
                    end,
                    
                    maps:put({X, Y}, NextState, Acc);
                false ->
                    Acc
            end
        end,
        #{},
        Grid
    ).

%% @doc Count live neighbors for a cell
count_live_neighbors(X, Y, Grid) ->
    Offsets = [
        {-1, -1}, {0, -1}, {1, -1},
        {-1,  0},          {1,  0},
        {-1,  1}, {0,  1}, {1,  1}
    ],
    
    lists:foldl(
        fun({DX, DY}, Count) ->
            NX = X + DX,
            NY = Y + DY,
            case maps:get({NX, NY}, Grid, false) of
                true -> Count + 1;
                false -> Count
            end
        end,
        0,
        Offsets
    ).

%% @doc Prints the current state of the grid to the terminal
print_grid(Blocks) ->
    % Clear screen with ANSI escape code (can be commented out if it doesn't work in your terminal)
    io:format("\033[2J"),
    % Move cursor to top-left corner
    io:format("\033[H"),
    
    io:format("Collecting block states for display...~n"),
    
    % Request state from all blocks
    maps:map(fun(BlockId, Pid) -> Pid ! {get_state, self(), BlockId} end, Blocks),
    
    % Collect all block states with a timeout
    AllBlocks = collect_block_states(maps:size(Blocks), #{}, make_ref()),
    
    % Verify we got data back
    case maps:size(AllBlocks) of
        0 ->
            io:format("Failed to collect block states. Check if simulation is running properly.~n");
        Count ->
            io:format("Collected states from ~p blocks.~n", [Count]),
            % Print a more compact representation (e.g., 50x50 characters for the 1000x1000 grid)
            % Each character will represent a 20x20 area (compressing the display)
            print_compressed_grid(AllBlocks)
    end.

%% @doc Collect state data from all blocks
collect_block_states(0, Acc, _) ->
    Acc;
collect_block_states(Remaining, Acc, Ref) ->
    receive
        {block_state, BlockId, Grid} ->
            collect_block_states(Remaining - 1, maps:put(BlockId, Grid, Acc), Ref)
    after 1000 ->
        io:format("Timeout waiting for block states~n"),
        Acc
    end.

%% @doc Print a compressed representation of the grid
print_compressed_grid(AllBlocks) ->
    % Error check - make sure we have blocks to print
    case maps:size(AllBlocks) of
        0 ->
            io:format("No block data available to print~n");
        _ ->
            % Compression factor - each character represents a 20x20 cell area
            ScaleFactor = 20,
            
            % Calculate dimensions of the compressed grid
            CompressedWidth = ?TOTAL_WIDTH div ScaleFactor,
            CompressedHeight = ?TOTAL_HEIGHT div ScaleFactor,
            
            % Print the compressed grid
            io:format("Game of Life State (each character represents a ~px~p cell area):~n", 
                      [ScaleFactor, ScaleFactor]),
            
            % Print the header (column numbers)
            io:format("    "),
            lists:foreach(
                fun(X) ->
                    % Print column number every 10 columns
                    case X rem 10 of
                        0 -> io:format("~p", [X div 10 rem 10]);
                        _ -> io:format(" ")
                    end
                end,
                lists:seq(0, CompressedWidth - 1)
            ),
            io:format("~n"),
                        
            % Print the grid with row numbers
            lists:foreach(
                fun(Y) ->
                    % Print row number
                    io:format("~3.B |", [Y]),
                    
                    % Print one row of the compressed grid
                    Line = lists:map(
                        fun(X) ->
                            % Calculate density of live cells in this 20x20 area
                            Density = calculate_cell_density(X * ScaleFactor, Y * ScaleFactor, 
                                                             ScaleFactor, AllBlocks),
                            % Map density to character
                            density_to_char(Density)
                        end,
                        lists:seq(0, CompressedWidth - 1)
                    ),
                    io:format("~s~n", [Line])
                end,
                lists:seq(0, CompressedHeight - 1)
            ),
            
            % Print legend
            io:format("~nLegend: [space]=0%, .=<20%, ·=<40%, o=<60%, O=<80%, #=>80% cell density~n")
    end.

%% @doc Calculate the density of live cells in a given area
calculate_cell_density(StartX, StartY, Size, AllBlocks) ->
    try
        % Count live cells in the area
        Count = lists:sum(
            lists:map(
                fun(Y) ->
                    lists:sum(
                        lists:map(
                            fun(X) ->
                                % Calculate which block this cell belongs to
                                BlockX = X div ?BLOCK_SIZE,
                                BlockY = Y div ?BLOCK_SIZE,
                                % Calculate local coordinates within the block
                                LocalX = X rem ?BLOCK_SIZE,
                                LocalY = Y rem ?BLOCK_SIZE,
                                
                                % Get the cell state
                                case maps:find({BlockX, BlockY}, AllBlocks) of
                                    {ok, Grid} ->
                                        case maps:find({LocalX, LocalY}, Grid) of
                                            {ok, true} -> 1;
                                            _ -> 0
                                        end;
                                    _ -> 0
                                end
                            end,
                            lists:seq(StartX, StartX + Size - 1)
                        )
                    )
                end,
                lists:seq(StartY, StartY + Size - 1)
            )
        ),
        
        % Calculate density as percentage
        Count / (Size * Size)
    catch
        Type:Error ->
            io:format("Error calculating density at (~p,~p): ~p:~p~n", 
                     [StartX, StartY, Type, Error]),
            % Return 0 density on error
            0.0
    end.

%% @doc Convert density value to a character for display
density_to_char(Density) ->
    % Use different characters based on cell density
    if
        Density =:= +0.0 -> $\s;           % Space for empty areas
        Density < 0.2 -> $.;              % Low density
        Density < 0.4 -> $·;              % Low-medium density
        Density < 0.6 -> $o;              % Medium density
        Density < 0.8 -> $O;              % Medium-high density
        true -> $#                         % High density
    end.