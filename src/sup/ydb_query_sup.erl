%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc TODO 
-module(ydb_query_sup).
-behaviour(supervisor).

-export([start_link/0, register/2]).
-export([init/1]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

%-spec TODO

%% @doc TODO
start_link() ->
    supervisor:start_link(?MODULE, {})
.

%-spec TODO

%% @doc TODO
register(SupRef, {Name, Query}) ->
    supervisor:start_child(
        SupRef

      , {
            Name
          , {ydb_query, start_link, [Query]}

          , transient
          , infinity % supervisor
          , supervisor
          , [ydb_query]
        }
    )
.

%% ----------------------------------------------------------------- %%

%-spec TODO

%% @doc TODO
init({}) ->
    {ok, {{one_for_one, 1, 60}, []}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%



%% ----------------------------------------------------------------- %%
