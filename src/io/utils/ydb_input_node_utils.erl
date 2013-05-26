%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc This module contains utility functions used for inputting
%%      tuples.
-module(ydb_input_node_utils).

-export([make_tuples/3, make_tuple/3]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

% Testing for private functions.
-ifdef(TEST).
-export([new_tuple/2]).
-endif.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec make_tuples(
    Timestamp :: {Unit :: atom(), Name :: atom()}
  , Schema :: list()
  , Data :: list()
) -> Tuples :: list().

%% @doc Makes a list of tuples of a given schema and set of data.
make_tuples(Timestamp, Schema, Data) when is_list(Data) ->
    lists:map(fun (Datum) when is_tuple(Datum) ->
        make_tuple(Timestamp, Schema, Datum) end, Data
    )
.

%% ----------------------------------------------------------------- %%

-spec make_tuple(
    Timestamp :: {Unit :: atom(), Name :: atom()}
  , Schema :: ydb_plan_node:ydb_schema()
  , Data :: tuple()
) -> Tuple :: ydb_plan_node:ydb_tuple().

%% @doc Makes a new tuple with a timestamp if not given.
make_tuple('$auto_timestamp', _Schema, Data) when is_tuple(Data) ->
    Timestamp = ydb_time_utils:get_curr_time()
  , new_tuple(Timestamp, Data)
;

make_tuple({Unit, Name}, Schema, Data)
  when
    is_atom(Name)
  , is_list(Schema)
  , is_tuple(Data)
  ->
    {Index, _Type} = dict:fetch(Name, dict:from_list(Schema))
  , Timestamp = ydb_time_utils:convert_time(
        {Unit, erlang:element(Index, Data)}
    )
  , new_tuple(Timestamp, Data)
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec new_tuple(Timestamp :: non_neg_integer(), Data :: tuple()) ->
    Tuple :: ydb_plan_node:ydb_tuple()
.

%% @doc Creates a new tuple.
new_tuple(Timestamp, Data)
  when
    is_integer(Timestamp), Timestamp >= 0
  , is_tuple(Data)
  ->
    #ydb_tuple{
        timestamp=Timestamp
      , data=Data
    }
.

%% ----------------------------------------------------------------- %%
