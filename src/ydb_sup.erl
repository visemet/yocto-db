-module(ydb_sup).
-behaviour(supervisor).

-export([start_link/0, register_input_stream/1, register_query/1]).
-export([init/1]).

-define(INPUT_STREAM_SUP, ydb_input_stream_sup).
-define(QUERY_SUP, ydb_query_sup).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

start_link() ->
    supervisor:start_link(?MODULE, {})
.

register_input_stream(Spec) ->
    ydb_input_stream_sup:register(?INPUT_STREAM_SUP, Spec)
.

register_query(Spec) ->
    ydb_query_sup:register(?QUERY_SUP, Spec)
.

%% ----------------------------------------------------------------- %%

init({}) ->
    {ok, {
        {one_for_one, 1, 60}

      , [
            {
                ?INPUT_STREAM_SUP
              , {?INPUT_STREAM_SUP, start_link, [?INPUT_STREAM_SUP]}
              , transient
              , infinity % supervisor
              , supervisor
              , [?INPUT_STREAM_SUP]
            }

          , {
                ?QUERY_SUP
              , {?QUERY_SUP, start_link, [?QUERY_SUP]}
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
