%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc TODO 
-module(ydb_query).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

%-spec TODO

%% @doc TODO
start_link(QuerySpec) ->
    supervisor:start_link(?MODULE, {QuerySpec})
.

%% ----------------------------------------------------------------- %%

%-spec TODO

%% @doc TODO
init({QuerySpec}) ->
    case ydb_planner:make(QuerySpec) of
        {ok, ChildSpec} -> {ok, {{one_for_one, 1, 60}, ChildSpec}}

      ; {error, _Reason} -> ignore
    end
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%



%% ----------------------------------------------------------------- %%
