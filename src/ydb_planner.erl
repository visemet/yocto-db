-module(ydb_planner).

-export([make_no_branch/1]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

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
