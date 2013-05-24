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
    {Type = select, Predicate, Prev}
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
                ydb_select, start_link, [
                    [{predicate, Predicate}]
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

make(
    {Type = project, Columns, Prev}
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
                ydb_project, start_link, [
                    [{columns, Columns}]
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

make(
    {{aggr, Type, Options}, Args, Prev}
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

          , Name = {name, Type}
          , {PrFun, AggrFun} = ydb_aggr_funs:get_aggr([Name|Options])

          , Pr = {pr_fun, PrFun}
          , Aggr = {aggr_fun, AggrFun}
          , CombinedArgs = lists:append(Args, [Pr, Aggr])

          , ChildSpec = prepare_child_spec(CurrId, {
                ydb_aggr_node, start_link, [
                    CombinedArgs
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

make(
    {Type = time_window, Size, Pulse, Prev}
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
                ydb_time_window, start_link, [
                    [{size, Size}, {pulse, Pulse}]
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

make(
    {Type = row_window, Size, Pulse, Prev}
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
                ydb_row_window, start_link, [
                    [{size, Size}, {pulse, Pulse}]
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

make(
    {
        Type = join
      , LeftSize
      , LeftPulse
      , RightSize
      , RightPulse
      , LeftPrev
      , RightPrev
    }
  , History
  , Result
) when
    is_tuple(LeftPrev)
  , is_tuple(RightPrev)
  , is_list(Result)
  ->
    case make(LeftPrev, History, Result) of
        {ok, {LeftPrevId, LeftNewHistory, LeftNewResult}} ->
            case make(RightPrev, LeftNewHistory, LeftNewResult) of
                {ok, {RightPrevId, RightNewHistory, RightNewResult}} ->
                    CurrId = get_id(Type, RightNewHistory)
                  , ListenA = prepare_listen(LeftPrevId)
                  , ListenB = prepare_listen(RightPrevId)

                  , ChildSpec = prepare_child_spec(CurrId, {
                        ydb_join, start_link, [
                            [
                                {left, {size, LeftSize}, {pulse, LeftPulse}}
                              , {right, {size, RightSize}, {pulse, RightPulse}}
                            ]
                          , [{listen, [ListenA, ListenB]}]
                        ]
                    })

                  , {ok, {
                        CurrId
                      , dict:update_counter(Type, 1, RightNewHistory)
                      , [ChildSpec|RightNewResult]}
                    }

              ; {error, Reason} -> {error, Reason}
            end

      ; {error, Reason} -> {error, Reason}
    end
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

make(
    {Type = socket_output, PortNo, Addr, Prev}
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
                ydb_socket_output, start_link, [
                    [{port_no, PortNo}, {address, Addr}]
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

make(
    {Type = http_output, Addr, Prev}
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
                ydb_http_output, start_link, [
                    [{address, Addr}]
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
