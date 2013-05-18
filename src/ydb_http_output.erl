%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module receives tuples from the query planner and outputs
%%      it to a socket.
-module(ydb_http_output).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(http_output, {address :: string()}).

-type http_output() :: #http_output{address :: undefined | string()}.
%% Internal http output node state.

-type option() :: {address, string()}.
%% Options for the http output:
%% <ul>
%%   <li><code>{address, Address}</code> - String containing the web
%%       address to send tuples to.</li<
%% </ul>

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(Args :: [option()], Options :: list()) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the output node in the supervisor hierarchy.
start_link(Args, Options) ->
    ydb_plan_node:start_link(?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: http_output()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the output node's internal state.
init(Args) when is_list(Args) -> init(Args, #http_output{}).

%% ----------------------------------------------------------------- %%

-spec delegate(Request :: atom(), State :: http_output()) ->
    {ok, State :: #http_output{}}
.

%% @private
%% @doc Sends received tuples out to the sepcified address.
delegate(
    _Request = {tuples, Tuples}
  , State = #http_output{address = Address}
) when is_list(Tuples)
->
    Encode = bert:encode(Tuples)
  , Data = "data=" ++ lists:flatten(io_lib:format("~w", [Encode]))
  , httpc:request(post, {
        Address, [], "application/x-www-form-urlencoded", Data
    }, [], [])
  , {ok, State}
;

delegate(_Request = {info, Message}, State) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: atom()
  , State :: http_output()
  , Extras :: list()
) ->
    {ok, NewState :: http_output()}
.

%% @private
%% @doc Not implemented.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: http_output()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the http output node based upon
%%      the supplied input schemas. Expects a single schema.
compute_schema([Schema], #http_output{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #http_output{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: http_output()) ->
    {ok, NewState :: http_output()}
  | {error, {badarg, Term :: term()}}
  | {error, Reason :: term()}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the input node.
init([], State = #http_output{}) ->
    % Start up inets.
    inets:start()
  , {ok, State}
;

init([{address, Address} | Args], State = #http_output{}) ->
    init(Args, State#http_output{address=Address})
;

init([Term | _Args], #http_output{}) ->
    {error, {badarg, Term}}
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #http_output{})
    )
.
-endif.
