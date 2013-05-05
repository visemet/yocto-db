-module(ydb_planner).

-export([make/1, make_nonjoin/1, make_no_branch/1]).

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

%-spec

%% @doc TODO
make_nonjoin({input_stream, Query, Name, Type})
  when
    is_tuple(Query)
  , is_atom(Name)
  , is_atom(Type)
  ->
    make_nonjoin(Query, {Name, Type}, dict:new(), [])
.

%-spec

%% @doc TODO
make_no_branch({input_stream, Query, Name})
  when
    is_tuple(Query)
  , is_atom(Name)
  ->
    make_no_branch(Query, Name, dict:new(), [])
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

%% ----------------------------------------------------------------- %%

%-spec

%% @doc TODO
make_nonjoin({}, _PrevId, _History, Result) ->
    {ok, lists:reverse(Result)}
;

make_nonjoin(
    {Type = select, Next, Predicate}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_atom(PrevId) orelse is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = case PrevId of
        % Listens to an input stream
        {InputStream, BranchType}
          when
            is_atom(InputStream)
          , is_atom(BranchType)
          ->
            {ydb_sup_utils:get_branch_pid(InputStream), BranchType}

        % Listens to another node in the query
      ; {PrevType, PrevValue}
          when
            is_atom(PrevType)
          , is_integer(PrevValue)
          ->
            ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_select, start_link, [
        [{predicate, Predicate}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_nonjoin(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_nonjoin(
    {Type = project, Next, Columns}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_atom(PrevId) orelse is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = case PrevId of
        % Listens to an input stream
        {InputStream, BranchType}
          when
            is_atom(InputStream)
          , is_atom(BranchType)
          ->
            {ydb_sup_utils:get_branch_pid(InputStream), BranchType}

        % Listens to another node in the query
      ; {PrevType, PrevValue}
          when
            is_atom(PrevType)
          , is_integer(PrevValue)
          ->
            ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_project, start_link, [
        [{columns, Columns}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_nonjoin(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_nonjoin(
    {Type = time_window, Next, Size, Pulse}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = case PrevId of
        % Listens to an input stream
        {InputStream, BranchType}
          when
            is_atom(InputStream)
          , is_atom(BranchType)
          ->
            {ydb_sup_utils:get_branch_pid(InputStream), BranchType}

        % Listens to another node in the query
      ; {PrevType, PrevValue}
          when
            is_atom(PrevType)
          , is_integer(PrevValue)
          ->
            ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_time_window, start_link, [
        [{size, Size}, {pulse, Pulse}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_nonjoin(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_nonjoin(
    {Type = row_window, Next, Size, Pulse}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = case PrevId of
        % Listens to an input stream
        {InputStream, BranchType}
          when
            is_atom(InputStream)
          , is_atom(BranchType)
          ->
            {ydb_sup_utils:get_branch_pid(InputStream), BranchType}

        % Listens to another node in the query
      ; {PrevType, PrevValue}
          when
            is_atom(PrevType)
          , is_integer(PrevValue)
          ->
            ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_row_window, start_link, [
        [{size, Size}, {pulse, Pulse}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_nonjoin(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_nonjoin(
    {Type = file_output, Next, Filename}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = case PrevId of
        % Listens to an input stream
        {InputStream, BranchType}
          when
            is_atom(InputStream)
          , is_atom(BranchType)
          ->
            {ydb_sup_utils:get_branch_pid(InputStream), BranchType}

        % Listens to another node in the query
      ; {PrevType, PrevValue}
          when
            is_atom(PrevType)
          , is_integer(PrevValue)
          ->
            ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_file_output, start_link, [
        [{filename, Filename}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_nonjoin(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_nonjoin(
    {Type = socket_output, Next, PortNo, Addr}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_atom(PrevId) orelse is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = case PrevId of
        % Listens to an input stream
        {InputStream, BranchType}
          when
            is_atom(InputStream)
          , is_atom(BranchType)
          ->
            {ydb_sup_utils:get_branch_pid(InputStream), BranchType}

        % Listens to another node in the query
      ; {PrevType, PrevValue}
          when
            is_atom(PrevType)
          , is_integer(PrevValue)
          ->
            ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_socket_output, start_link, [
        [{port_no, PortNo}, {address, Addr}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_nonjoin(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_nonjoin(Query, _PrevId, _History, _Result) ->
    {error, {badarg, Query}}
.

%% ----------------------------------------------------------------- %%

%-spec

%% @doc TODO
make_no_branch({}, _PrevId, _History, Result) ->
    {ok, lists:reverse(Result)}
;

make_no_branch(
    {Type = select, Next, Predicate}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_atom(PrevId) orelse is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = if
        % Listens to an input stream
        is_atom(PrevId) -> PrevId

        % Listens to another node in the query
      ; is_tuple(PrevId) -> ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_select, start_link, [
        [{predicate, Predicate}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_no_branch(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_no_branch(
    {Type = project, Next, Columns}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_atom(PrevId) orelse is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = if
        % Listens to an input stream
        is_atom(PrevId) -> PrevId

        % Listens to another node in the query
      ; is_tuple(PrevId) -> ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_project, start_link, [
        [{columns, Columns}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_no_branch(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_no_branch(
    {Type = file_output, Next, Filename}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_atom(PrevId) orelse is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = if
        % Listens to an input stream
        is_atom(PrevId) -> PrevId

        % Listens to another node in the query
      ; is_tuple(PrevId) -> ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_file_output, start_link, [
        [{filename, Filename}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_no_branch(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_no_branch(
    {Type = socket_output, Next, PortNo, Addr}
  , PrevId
  , History
  , Result
) when
    is_tuple(Next)
  , is_atom(PrevId) orelse is_tuple(PrevId)
  , is_list(Result)
  ->
    CurrId = case dict:find(Type, History) of
        {ok, Value} -> {Type, Value}

      ; error -> {Type, 0}
    end

  , Listen = if
        % Listens to an input stream
        is_atom(PrevId) -> PrevId

        % Listens to another node in the query
      ; is_tuple(PrevId) -> ydb_sup_utils:pid_fun(PrevId)
    end

  , ChildSpec = prepare_child_spec(CurrId, {ydb_socket_output, start_link, [
        [{port_no, PortNo}, {address, Addr}]
      , [{listen, [Listen]}]
    ]})

  , NewHistory = dict:update_counter(Type, 1, History)

  , make_no_branch(Next, CurrId, NewHistory, [ChildSpec|Result])
;

make_no_branch(Query, _PrevId, _History, _Result) ->
    {error, {badarg, Query}}
.

%% ----------------------------------------------------------------- %%

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
