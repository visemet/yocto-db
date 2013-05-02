%% Header file for time-related definitions.

-export_type([time_unit/0]).

%% Number of seconds in a megasecond.
-define(SECS_PER_MEGASEC, 1000000).
%% Number of microseconds in a second.
-define(MICROSECS_PER_SEC, 1000000).

%% Number of microseconds in a millisecond.
-define(MICROSECS_PER_MILLISEC, 1000).
%% Number of milliseconds in a second.
-define(MILLISECS_PER_SEC, 1000).
%% Number of seconds in a minute.
-define(SECS_PER_MIN, 60).
%% Number of minutes in an hour.
-define(MINS_PER_HOUR, 60).
%% Number of hours in a day.
-define(HOURS_PER_DAY, 24).

%% Number of days in a week.
-define(DAYS_PER_WEEK, 7).
%% Number of days in a month.
-define(DAYS_PER_MONTH, 30).
%% Number of days in a year.
-define(DAYS_PER_YEAR, 365).

%% ----------------------------------------------------------------- %%

-type time_unit() ::
    'micro_sec'
  | 'milli_sec'
  | 'sec'
  | 'min'
  | 'hour'
  | 'day' 
  | 'week'
  | 'month'
  | 'year'
.
%% Time units, which can be one of the following:
%% <ul>
%%   <li>`micro_sec' = microseconds</li>
%%   <li>`milli_sec' = milliseconds</li>
%%   <li>`sec' = seconds</li>
%%   <li>`min' = minutes</li>
%%   <li>`hour' = hours</li>
%%   <li>`day' = days</li>
%%   <li>`week' = weeks</li>
%%   <li>`month' = months</li>
%%   <li>`year' = years</li>
%% </ul>

%% ----------------------------------------------------------------- %%
