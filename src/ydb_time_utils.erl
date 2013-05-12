%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Utilities for the time-related operations.

-module(ydb_time_utils).

-export([get_curr_time/0, convert_time/1]).

%% @headerfile "ydb_time.hrl"
-include("ydb_time.hrl").

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec get_curr_time() -> CurrTimeInMicroSecs :: non_neg_integer().

%% @doc Returns the current time in microseconds.
get_curr_time() ->
    {MegaSecs, Secs, MicroSecs} = erlang:now()
  , (MegaSecs * ?SECS_PER_MEGASEC + Secs) * ?MICROSECS_PER_SEC + MicroSecs
.

%% ----------------------------------------------------------------- %%

-spec convert_time({
    Unit :: time_unit()
  , TimeInUnit :: number()
}) ->
    TimeInMicroSecs :: non_neg_integer()
  | {error, {badarg, term()}}
.

%% @doc Converts a time to microseconds.
convert_time({micro_sec, MicroSecs}) when MicroSecs >= 0 ->
    erlang:trunc(MicroSecs)
;

convert_time({milli_sec, MilliSecs}) when MilliSecs >= 0 ->
    convert_time({micro_sec, MilliSecs * ?MICROSECS_PER_MILLISEC})
;

convert_time({sec, Secs}) when Secs >= 0 ->
    convert_time({milli_sec, Secs * ?MILLISECS_PER_SEC})
;

convert_time({min, Mins}) when Mins >= 0 ->
    convert_time({sec, Mins * ?SECS_PER_MIN})
;

convert_time({hour, Hours}) when Hours >= 0 ->
    convert_time({min, Hours * ?MINS_PER_HOUR})
;

convert_time({day, Days}) when Days >= 0 ->
    convert_time({hour, Days * ?HOURS_PER_DAY})
;

convert_time({week, Weeks}) when Weeks >= 0 ->
    convert_time({day, Weeks * ?DAYS_PER_WEEK})
;

convert_time({month, Months}) when Months >= 0 ->
    convert_time({day, Months * ?DAYS_PER_MONTH})
;

convert_time({year, Years}) when Years >= 0 ->
    convert_time({day, Years * ?DAYS_PER_YEAR})
;

convert_time(Arg) ->
    {error, {badarg, Arg}}
.

%% ----------------------------------------------------------------- %%
