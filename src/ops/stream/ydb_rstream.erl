%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Module for converting a diff into a stream. Produces the
%%      current representation of the relation.
-module(ydb_rstream).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-record(rstream, {
    synopsis=new_synopsis() :: ets:tid()
}).

-type rstream() :: #rstream{
    synopsis :: ets:tid()
}.
%% Internal state of the rstream node.

-type option() :: {}.
%% No options.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(
    Args :: [option()]
  , Options :: list()
) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the rstream node in the supervisor hierarchy.
start_link(Args, Options) ->
    ydb_plan_node:start_link(?MODULE, Args, Options)
.

-spec start_link(
    Name :: atom()
  , Args :: [option()]
  , Options :: list()
) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the rstream node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: rstream()}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the internal state of the rstream node.
init(Args) when is_list(Args) -> init(Args, #rstream{}).

-spec delegate(Request :: term(), State :: rstream()) ->
    {ok, State :: rstream()}
.

%% @doc Produces the current representation of the relation from the
%%      diffs.
delegate({diffs, Diffs}, State = #rstream{
    synopsis = Synopsis
}) when
    is_list(Diffs)
  ->
    lists:foreach(
        fun (Diff) ->
            {Plus, Minus} = ydb_ets_utils:extract_diffs([Diff])

          , lists:foreach(
                fun (Tuple) ->
                    do_insert(Synopsis, Tuple)
                end

              , Plus
            )

          , lists:foreach(
                fun (Tuple) ->
                    do_delete(Synopsis, Tuple)
                end

              , Minus
            )

          , Relation = get_relation(Synopsis)
          , ydb_plan_node:send_tuples(erlang:self(), Relation)
        end

      , Diffs
    )

  , {ok, State}
;

delegate({info, Message}, State = #rstream{}) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: term()
  , State :: rstream()
  , Extras :: list()
) ->
    {ok, NewState :: rstream()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: rstream()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the rstream node based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #rstream{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #rstream{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: rstream()) ->
    {ok, NewState :: rstream()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the internal state of the rstream node.
init([], State = #rstream{}) ->
    {ok, State}
;

init([Term | _Args], #rstream{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec new_synopsis() -> Synopsis :: ets:tid().

%% @doc Returns a new ETS table used for storing the current
%%      representation of the relation.
new_synopsis() ->
    ets:new(synopsis, [set])
.

%% ----------------------------------------------------------------- %%

-spec do_insert(
    Synopsis :: ets:tid()
  , Tuple :: ydb_plan_node:ydb_tuple()
) ->
    ok
.

%% @doc Inserts the specified tuple into the table.
do_insert(Synopsis, Tuple) ->
    try ets:update_counter(Synopsis, Tuple, {2, 1}) of
        % A copy of the tuple already existed in the table
        Count when is_integer(Count), Count > 1 -> pass
    catch
        % No object with the right key exists
        error:badarg ->
            % Insert key=`Tuple' into the table with value=`1'
            ets:insert(Synopsis, {Tuple, 1})
    end

  , ok
.

-spec do_delete(
    Synopsis :: ets:tid()
  , Tuple :: ydb_plan_node:ydb_tuple()
) ->
    ok
.

%% @doc Deletes the specified tuple from the table.
do_delete(Synopsis, Tuple) ->
    case ets:update_counter(Synopsis, Tuple, {2, -1, 0, 0}) of
        % Last copy of the tuple was removed
        Count when is_integer(Count), Count =:= 0 ->
            % Delete key=`Tuple' from the table
            ets:delete(Synopsis, Tuple)

        % A copy of the tuple already existed in the table
      ; Count when is_integer(Count), Count > 0 -> pass
    end

  , ok
.

%% ----------------------------------------------------------------- %%

-spec get_relation(Synopsis :: ets:tid()) ->
    Relation :: [ydb_plan_node:ydb_tuple()]
.

%% @doc Returns the current representation of the relation.
get_relation(Synopsis) ->
    lists:append(lists:map(
        fun ({Tuple, Count}) ->
            lists:duplicate(Count, Tuple)
        end

      , lists:append(ets:match(Synopsis, '$1'))
    ))
.

%% ----------------------------------------------------------------- %%
