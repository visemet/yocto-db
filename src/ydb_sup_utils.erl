-module(ydb_sup_utils).

-export([pid_fun/1, get_pid/2, get_branch_pid/1]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec pid_fun(
    ChildId :: term() % supervisor:child_id()
) ->
    fun(() -> ChildPid :: pid())
.

%% @doc TODO
pid_fun(ChildId) when not is_pid(ChildId) ->
    SupRef = erlang:self()

  , fun() ->
        get_pid(SupRef, ChildId)
    end
.

-spec get_pid(
    SupRef :: pid() | atom() % supervisor:sup_ref()
  , ChildId :: term() % supervisor:child_id()
) ->
    ChildPid :: pid()
.

%% @doc TODO
get_pid(SupRef, ChildId)
  when
    is_pid(SupRef) orelse is_atom(SupRef)
  , not is_pid(ChildId)
  ->
    erlang:element(
        2 % {Id, **Child, Type, Modules}
      , lists:keyfind(
            ChildId
          , 1 % {**Id, Child, Type, Modules}
          , supervisor:which_children(SupRef)
        )
    )
.

%-spec

%% @doc TODO
get_branch_pid(InputStream) when is_atom(InputStream) ->
    get_pid(                                   % ydb_input_stream
        get_pid(                               % ydb_input_stream_sup
            get_pid(ydb, ydb_input_stream_sup) % ydb_sup
          , InputStream
        )

      , ydb_branch_node
    )
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%



%% ----------------------------------------------------------------- %%
