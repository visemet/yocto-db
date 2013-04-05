%% @author Max Hirschhorn <maxh@caltech.edu>
%%
%% @doc This module contains utility functions used for inputting
%%      tuples.
-module(ydb_input_node_utils).

-export([make_tuples/3, make_tuple/3, push/1]).

-include("ydb_plan_node.hrl").

% Number of seconds in a megasecond.
-define(MegaSecs_To_Secs, 1000000).
% Number of microseconds in a second.
-define(Secs_To_MicroSecs, 1000000).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec make_tuples(
    Timestamp :: integer()
  , Schema :: list()
  , Data :: list()
) ->
    Tuples :: list()
.

%% @doc Makes a list of tuples of a given schema and set of data.
make_tuples(Timestamp, Schema, Data) when is_list(Data) ->
    lists:map(
        fun (Datum) when is_tuple(Datum) ->
            make_tuple(Timestamp, Schema, Datum)
        end

      , Data
    )
.

%% ----------------------------------------------------------------- %%

-spec make_tuple(
    Timestamp :: {Unit :: atom(), Name :: atom()}
  , Schema :: list()
  , Data :: tuple()
) ->
    Tuple :: ydb_tuple()
.

%% @doc Makes a new tuple with a timestamp if not given.
make_tuple('$auto_timestamp', _Schema, Data) when is_tuple(Data) ->
    Timestamp = convert_time(get_curr_time())

  , new_tuple(Timestamp, Data)
;

make_tuple({Unit, Name}, Schema, Data)
  when
    is_atom(Name)
  , is_list(Schema)
  , is_tuple(Data)
  ->
    {Index, _Type} = dict:fetch(Name, dict:from_list(Schema))
  , Timestamp = convert_time({Unit, erlang:element(Index, Data)})

  , new_tuple(Timestamp, Data)
.

%% ----------------------------------------------------------------- %%

-spec push(Tuple :: ydb_tuple()) -> ok.

%% @doc TODO
push(Tuple = #ydb_tuple{}) ->
    ydb_plan_node:notify(
        erlang:self()
      , {tuple, Tuple}
    )
;

push(Tuples) when is_list(Tuples) ->
    ydb_plan_node:notify(
        erlang:self()
      , {tuples, Tuples}
    )
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec new_tuple(Timestamp :: integer(), Data :: tuple()) ->
    Tuple :: ydb_tuple()
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

-spec get_curr_time() -> {Unit :: atom(), TimeInMicroSecs :: integer()}.

%% @doc Gets the current time in microseconds.
get_curr_time() ->
    {MegaSecs, Secs, MicroSecs} = erlang:now()

  , {
        micro_sec
      , (MegaSecs * ?MegaSecs_To_Secs + Secs) * ?Secs_To_MicroSecs + MicroSecs
    }
.

%% ----------------------------------------------------------------- %%

-spec convert_time({Unit :: atom(), TimeInSecs :: integer()}) ->
    {TimeInMicroSecs :: integer()}
  | {error, {badarg, Unit :: atom()}}
.

%% @doc Converts a time to microseconds.
convert_time({micro_sec, MicroSecs}) ->
    MicroSecs
;

convert_time({milli_sec, MilliSecs}) ->
    convert_time({micro_sec, MilliSecs * 1000})
;

convert_time({sec, Secs}) ->
    convert_time({milli_sec, Secs * 1000})
;

convert_time({min, Mins}) ->
    convert_time({sec, Mins * 60})
;

convert_time({hour, Hours}) ->
    convert_time({min, Hours * 60})
;

convert_time({Unit, _Time}) ->
    {error, {badarg, Unit}}
.

%% ----------------------------------------------------------------- %%
