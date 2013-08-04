%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Extensions to the `lists' module.
-module(ydb_lists_ext).

-export([foldli/3]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec foldli(
    Fun :: fun((Index :: pos_integer()
              , Elem :: term()
              , AccIn :: term()) -> AccOut :: term())
  , Acc0 :: term()
  , List :: [term()]
) ->
    Acc1 :: term()
.

%% @doc Similar to `lists:foldl/3', where the provided function takes
%% an additional parameter for the index of the element in the list.
foldli(Fun, InitAcc, List) when is_function(Fun, 3), is_list(List) ->
    {_Length, Result} = lists:foldl(
        fun (Elem, {Index0, Result0}) ->
            Index1 = Index0 + 1
          , Result1 = Fun(Index1, Elem, Result0)
          , {Index1, Result1}
        end

      , {0, InitAcc}
      , List
    )

  , Result
.

%% ----------------------------------------------------------------- %%
