-module(ydb_sup_utils).

-export([get_pid/2]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec get_pid(
    SupRef :: pid() % supervisor:sup_ref()
  , ChildId :: atom() % supervisor:child_id()
) ->
    ChildPid :: pid()
.

%% @doc TODO
get_pid(SupRef, ChildId)
  when
    is_pid(SupRef)
  , is_atom(ChildId)
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
