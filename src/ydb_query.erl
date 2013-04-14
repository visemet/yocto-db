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
start_link(Query) ->
    supervisor:start_link(?MODULE, {Query})
.

%% ----------------------------------------------------------------- %%

%-spec TODO

%% @doc TODO
init({_Query}) ->
    % TODO call to planner
    {ok, {{one_for_one, 1, 60}, []}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%



%% ----------------------------------------------------------------- %%
