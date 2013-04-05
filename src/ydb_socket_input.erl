% @author Kalpana Suraesh
%
% @doc This module reads input from a socket and pushes it to the query planner.
-module(ydb_socket_input).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2, delegate/3]).

-record(socket_input, {port_no, socket}).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

start_link(Args, Options) ->
    ydb_plan_node:start_link(?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

init(Args) when is_list(Args) -> init(Args, #socket_input{}).

delegate(Request = {listen}, State = #socket_input{}) ->
    % @max, you had something like ydb_plan_node:notify() in ydb_file_input
    % here, but I can't figure out what it does...?
    gen_server:cast(erlang:self(), {delegate, Request, [schema, timestamp]})
  , {ok, State}
;

delegate(_Request, State) ->
    {ok, State}
.

delegate(
    Request = {listen}
  , State = #socket_input{socket=Socket}
  , _Extras = [Schema, Timestamp]
) ->
    inet:setopts(Socket, [{active, once}])
  , receive
        {tcp, Socket, BinData} ->
            ydb_input_node_utils:push(
                ydb_input_node_utils:make_tuple(Timestamp, Schema,
                    binary_to_term(BinData))
            )
    end
  , gen_server:cast(erlang:self(), {delegate, Request})
  , {ok, State};

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

init([], State = #socket_input{}) ->
    NewState = post_init(State)
  , {ok, NewState}
;

init([{port_no, Port} | Args], State = #socket_input{}) ->
    init(Args, State#socket_input{port_no=Port})
;

init([Term | _Args], #socket_input{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

post_init(State = #socket_input{port_no = PortNo}) ->
    {ok, LSock} = gen_tcp:listen(PortNo, [{active, false}, binary])
  , {ok, ASock} = gen_tcp:accept(LSock)
  , gen_server:cast(erlang:self(), {delegate, {listen}})
  , State#socket_input{socket=ASock}.

%% ----------------------------------------------------------------- %%
