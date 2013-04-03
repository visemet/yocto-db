-module(ydb_input_node).

-export([make_tuple/3, push/1]).

-include("ydb_plan_node.hrl").

-define(MegaSecs_To_Secs, 1000000).
-define(Secs_To_MicroSecs, 1000000).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

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
      , {'$gen_cast', {delegate, {tuple, Tuple}}}
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
