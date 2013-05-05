-module(ydb_planner).

-export([make/1]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

%-spec

%% @doc TODO
make(Query) when is_tuple(Query) ->
    case make(Query, dict:new(), []) of
        {ok, {_Id, _History, Result}} -> {ok, lists:reverse(Result)}

      ; {error, Reason} -> {error, Reason}
    end
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

%-spec

%% @doc TODO
make({input_stream, Name, Type}, History, Result) ->
    {ok, {{Name, Type}, History, Result}}
;

make(
    {Type = file_output, Filename, Prev}
  , History
  , Result
) when
    is_tuple(Prev)
  , is_list(Result)
  ->
    case make(Prev, History, Result) of
        {ok, {PrevId, NewHistory, NewResult}} ->
            CurrId = get_id(Type, NewHistory)
          , Listen = prepare_listen(PrevId)

          , ChildSpec = prepare_child_spec(CurrId, {
                ydb_file_output, start_link, [
                    [{filename, Filename}]
                  , [{listen, [Listen]}]
                ]
            })

          , {ok, {
                CurrId
              , dict:update_counter(Type, 1, NewHistory)
              , [ChildSpec|NewResult]}
            }

      ; {error, Reason} -> {error, Reason}
    end
;

make(Query, _History, _Result) ->
    {error, {badarg, Query}}
.

%% ----------------------------------------------------------------- %%

%-spec

%% @doc TODO
get_id(Type, History) ->
    case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end
.

%-spec

%% @doc TODO
prepare_listen(PlanId) ->
    case PlanId of
        % Listens to an input stream
        {InputStream, BranchType}
          when
            is_atom(InputStream)
          , is_atom(BranchType)
          ->
            {ydb_sup_utils:get_branch_pid(InputStream), BranchType}

        % Listens to another node in the query
      ; {PlanType, PlanValue}
          when
            is_atom(PlanType)
          , is_integer(PlanValue)
          ->
            ydb_sup_utils:pid_fun(PlanId)
    end
.

%-spec

%% @doc TODO
prepare_child_spec(Id, {Mod, Fun, Args}) ->
    {
        Id
      , {Mod, Fun, Args}
      , transient
      , 5000
      , worker
      , [Mod]
    }
.

%% ----------------------------------------------------------------- %%
