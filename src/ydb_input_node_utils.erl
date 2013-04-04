% @author Max Hirschhorn
%
% @doc This module contains utility functions used for inputting tuples.
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

make_tuples(Timestamp, Schema, Data) when is_list(Data) ->
    lists:map(
        fun (Datum) when is_tuple(Datum) ->
            make_tuple(Timestamp, Schema, Datum)
        end

      , Data
    )
.

%% ----------------------------------------------------------------- %%

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
    {Index, _Type} = dict:find(Name, dict:from_list(Schema))
  , Timestamp = convert_time({Unit, erlang:element(Index, Data)})

  , new_tuple(Timestamp, Data)
.

%% ----------------------------------------------------------------- %%

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

get_curr_time() ->
    {MegaSecs, Secs, MicroSecs} = erlang:now()

  , {
        micro_sec
      , (MegaSecs * ?MegaSecs_To_Secs + Secs) * ?Secs_To_MicroSecs + MicroSecs
    }
.

%% ----------------------------------------------------------------- %%

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
