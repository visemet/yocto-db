%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc TODO 
-module(ydb_input_stream).
-behaviour(supervisor).

-export([start_link/5]).
-export([init/1]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

%-spec TODO

%% @doc TODO
start_link(Type, Args, Options, Schemas, Timestamps) ->
    supervisor:start_link(
        ?MODULE
      , {{Type, Args, Options}, {Schemas, Timestamps}}
    )
.

%% ----------------------------------------------------------------- %%

%-spec TODO

%% @doc TODO
init({{Type, Args, Options}, {Schemas, Timestamps}}) ->
    {ok, {
        {one_for_one, 1, 60}

      , [
            {
                Type
              , {Type, start_link, [Args, Options]}
              , transient
              , brutal_kill
              , worker
              , [Type]
            }

          , {
                ydb_branch_node
              , {ydb_branch_node, start_link, [Schemas, Timestamps]}
              , permanent
              , brutal_kill
              , worker
              , [ydb_branch_node]
            }
        ]
    }}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%



%% ----------------------------------------------------------------- %%
