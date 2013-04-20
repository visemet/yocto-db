%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module serves as an interface for plan_nodes to interact
%%      with the ets table that serves as a synopsis.
-module(ydb_ets_reader).
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).
-export([start_link/3, start_link/4, get_copy/2, get_data_stream/2,
    add_tuples/2, delete_table/1]).
-export([coalesce/0, slide_window/0]).


% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%% =============================================================== %%%
%%%  internal records and types                                     %%%
%%% =============================================================== %%%

%% Internal ets reader node state.

-record(ets_reader, {
    table_name :: atom()
  , row_type :: atom()
  , table :: ets:tid()
  , count :: integer()
}).

-type ets_reader() :: #ets_reader{
    table_name :: atom()
  , row_type   :: atom()
  , table      :: ets:tid()
  , count      :: integer()}.

-type option() ::
    {table_name, atom()}
  | {row_type  , atom()}.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(atom(), term(), list()) ->
    {ok, pid()}
  | {error, term()}
.

%% @doc Starts the reader.
start_link(Type, Args, Options) ->
    gen_server:start_link(?MODULE, {Type, Args, Options}, [])
.

%% ----------------------------------------------------------------- %%

-spec start_link(atom(), atom(), term(), list()) ->
    {ok, pid()}
  | {error, term()}
.

%% @doc Starts the reader with a registered name.
start_link(Name, Type, Args, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, {Type, Args, Options}, [])
.

%% ----------------------------------------------------------------- %%

-spec get_copy(Pid :: pid(), Name :: atom()) -> {ok, Tid :: ets:tid()}.

%% @doc Creates a copy of all the tuples in this table and returns
%%      a reference to the new table.
get_copy(Pid, Name) ->
    gen_server:call(Pid, {get_copy, Name})
.

%% ----------------------------------------------------------------- %%

-spec get_data_stream(Pid :: pid(), ReturnPid :: pid()) -> ok.

%% @doc Sends out all the current data, after applying any inserts and
%%      deletes, to ReqPid
get_data_stream(Pid, ReqPid) ->
    gen_server:cast(Pid,{get_data_stream, ReqPid})
.

%% ----------------------------------------------------------------- %%

-spec add_tuples
    (Pid :: pid(), Tuples :: [tuple()]) -> ok
  ; (Pid :: pid(), Tuple :: tuple()) -> ok
.

%% @doc Adds the specified tuple(s) to the table
add_tuples(Pid, Tuples) when is_list(Tuples) ->
    gen_server:cast(Pid, {add_tuples, Tuples})
;
add_tuples(Pid, Tuple) when is_tuple(Tuple) ->
    add_tuples(Pid, [Tuple])
.

%% ----------------------------------------------------------------- %%

-spec delete_table(Pid :: pid()) -> ok.

%% @doc Deletes table.
delete_table(Pid) ->
    gen_server:cast(Pid, {delete_table})
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: {atom(), term(), list()}) ->
    {ok, ets_reader()}
  | {stop, term()}
.

%% @doc Initializes the internal state of the reader.
init({_Type, _Args, Options}) ->
    init(Options, #ets_reader{})
.

%% ----------------------------------------------------------------- %%

-spec handle_call(
    Request :: term()
  , From :: {pid(), Tag :: term()}
  , State :: ets_reader()
) ->
    {reply, Reply :: term(), NewState :: ets_reader()}
.

%% @doc TODO

% returns a copy of the current table
handle_call({get_copy, Name}, _From, State = #ets_reader{table=Tid}) ->
    {ok, NewTid} = do_copy_table(Tid, Name)
  , {reply, {ok, NewTid}, State}
;

handle_call(_Request, _From, State) ->
    {reply, ok, State}
.

%% ----------------------------------------------------------------- %%

-spec handle_cast(Request :: term(), State :: ets_reader()) ->
    {noreply, NewState :: ets_reader()}
.

%% @doc TODO

% adds tuples to the table
handle_cast(
    {add_tuples, Tuples}
  , State = #ets_reader{count=Count, table=Tid, row_type=RowType}
) ->
    {ok, NewCount} = do_add_tuples(Tuples, Count, RowType, Tid)
  , {noreply, State#ets_reader{count=NewCount}}
;

% sends data to the requesting pid
handle_cast({get_data_stream, ReqPid}, State = #ets_reader{table=Tid}) ->
    do_get_data_stream(ReqPid, Tid)
  , {noreply, State}
;

% deletes the table
handle_cast({delete_table}, State = #ets_reader{table=Tid}) ->
    do_delete_table(Tid)
  , {noreply, State}
;

handle_cast(_Request, State) ->
    {noreply, State}
.

%% ----------------------------------------------------------------- %%

-spec handle_info(Info :: timeout | term(), State :: ets_reader()) ->
    {noreply, NewState :: ets_reader()}
.

handle_info(_Info, State) ->
    {noreply, State}
.

%% ----------------------------------------------------------------- %%

-spec terminate(Reason :: term(), State :: ets_reader()) -> ok.

%% @doc Called by a gen_server when it is about to terminate.
%%      TODO: should delete ETS table here? Maybe have option to
%%            pass it on...
terminate(_Reason, _State) -> ok.

%% ----------------------------------------------------------------- %%

-spec code_change(
    OldVsn :: term()
  , State :: ets_reader()
  , Extra :: term()
) ->
    {ok, NewState :: ets_reader()}
.

%% @doc Called by a gen_server when it should update its interal state
%%      during a release upgrade or downgrade. Unsupported; the state
%%      remains the same.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: ets_reader()) ->
    {ok, State :: ets_reader()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the ets_reader node.
init([], State = #ets_reader{}) ->
    post_init(State)
;

init([{name, Name} | Args], State = #ets_reader{}) ->
    init(Args, State#ets_reader{table_name=Name})
;

init([{row_type, RType} | Args], State = #ets_reader{}) ->
    init(Args, State#ets_reader{row_type=RType})
;

init([Term | _Args], _State) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec post_init(State :: ets_reader()) -> {ok, NewState :: ets_reader()}.

%% @private
%% @doc Creates a new table and initializes the count to 0.
post_init(State = #ets_reader{table_name=Name}) ->
    NewState = State#ets_reader{count=0}
  , {ok, Tid} = create_table(Name)
  , {ok, NewState#ets_reader{table=Tid}}
.

%% ----------------------------------------------------------------- %%

-spec do_delete_table(Tid :: ets:tid()) -> ok.

%% @private
%% @doc Deletes the ets table.
do_delete_table(Tid) ->
    ets:delete(Tid)
  , ok
.

%% ----------------------------------------------------------------- %%

-spec do_add_tuples(
    Tuple :: [tuple()]
  , Count :: integer()
  , RowType :: atom()
  , Tid :: ets:tid()
) ->
    {ok, NewCount :: integer()}.

%% tuple should be {Type, TS, Data}

%% @private
%% @doc Adds a new tuple to the ets table.
do_add_tuples([], Count, _RowType, _Tid) ->
    {ok, Count}
;

do_add_tuples(
    [_Tuple = {Type, Timestamp, Data} | Tuples]
  , Count
  , RowType
  , Tid
) ->
    ets:insert(Tid, {{RowType, Count}, Type, Timestamp, Data})
  , do_add_tuples(Tuples, Count + 1, RowType, Tid)
.

%% ----------------------------------------------------------------- %%

-spec do_copy_table(
    Tid :: ets:tid()
  , Name :: atom()
) ->
    {ok, Tid :: ets:tid()}.

%% @private
%% @doc Returns a complete copy of the table.
do_copy_table(Tid, Name) ->
    NewTid = ets:new(Name, [])
  , Tuples = ets:match_object(Tid, '_')
  , ets:insert(NewTid, Tuples)
  , {ok, NewTid}
.



%% ----------------------------------------------------------------- %%

-spec do_get_data_stream(ReqPid :: pid(), Tid :: ets:tid()) -> {ok}.

%% @private
%% @doc Sends all the tuples as a stream to the requesting
%%      processes, after applying all edits (additions/deletions).
do_get_data_stream(ReqPid, Tid) ->
    Tuples = lists:map(fun(X) -> list_to_tuple(X) end,
                       ets:match(Tid, {'_', '$1', '$2', '$3'}))
  , ReqPid ! {tuples, Tuples}
  , {ok}
.

%% ----------------------------------------------------------------- %%

-spec create_table(Name :: atom()) -> {ok, Tid:: ets:tid()}.

%% @private
%% @doc Creates a new ets table with the given name.
create_table(Name) ->
    Tid = ets:new(Name, []) % TODO add options
  , {ok, Tid}
.

%% ----------------------------------------------------------------- %%

-spec coalesce() -> {ok}.

%% @private
%% @doc Applies all additions and deletions to the table.
coalesce() ->
    % TODO IMPLEMENT
    {ok}
.

%% ----------------------------------------------------------------- %%

-spec slide_window() -> {ok}.

%% @private
%% @doc Inserts "delete" tuples to simulate sliding the window.
slide_window() ->
    % TODO IMPLEMENT
    {ok}
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

%% TODO

%% ----------------------------------------------------------------- %%
