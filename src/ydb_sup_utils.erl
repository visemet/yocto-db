-module(ydb_sup_utils).

-export([pid_fun/1, get_pid/2]).

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
    SupRef :: pid() % supervisor:sup_ref()
  , ChildId :: term() % supervisor:child_id()
) ->
    ChildPid :: pid()
.

%% @doc TODO
get_pid(SupRef, ChildId)
  when
    is_pid(SupRef)
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

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%



%% ----------------------------------------------------------------- %%
