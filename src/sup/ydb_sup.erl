-module(ydb_sup).
-behaviour(supervisor).

-export([start_link/0, register_input_stream/1, register_query/1]).
-export([init/1]).

-define(INPUT_STREAM_SUP, ydb_input_stream_sup).
-define(QUERY_SUP, ydb_query_sup).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

%-spec

%% @doc TODO
start_link() ->
    supervisor:start_link({local, ydb}, ?MODULE, {})
.

%-spec

%% @doc TODO
register_input_stream(Spec) ->
    SupRef = ydb_sup_utils:get_pid(
        erlang:whereis(ydb)
      , ?INPUT_STREAM_SUP
    )

  , ydb_input_stream_sup:register(SupRef, Spec)
.

%-spec

%% @doc TODO
register_query(Spec) ->
    SupRef = ydb_sup_utils:get_pid(
        erlang:whereis(ydb)
      , ?QUERY_SUP
    )

  , ydb_query_sup:register(SupRef, Spec)
.

%% ----------------------------------------------------------------- %%

%-spec

%% @doc TODO
init({}) ->
    {ok, {
        {one_for_one, 1, 60}

      , [
            {
                ?INPUT_STREAM_SUP
              , {?INPUT_STREAM_SUP, start_link, []}
              , transient
              , infinity % supervisor
              , supervisor
              , [?INPUT_STREAM_SUP]
            }

          , {
                ?QUERY_SUP
              , {?QUERY_SUP, start_link, []}
              , transient
              , infinity % supervisor
              , supervisor
              , [?QUERY_SUP]
            }
        ]
    }}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%



%% ----------------------------------------------------------------- %%
