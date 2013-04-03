-module(ydb_file_input).
-behaviour(ydb_plan_node).

-export([start/3]).
-export([init/1, delegate/2]).

-record(file_input, {io_device, batch_size, poke_freq}).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

start(Name, Schema, Options) ->
    ydb_plan_node:start(Name, ?MODULE, Schema, Options)
.

%% ----------------------------------------------------------------- %%

init(Options) when is_list(Options) -> init(Options, #file_input{}).

delegate(
    Request = {read}
  , State = #file_input{
        io_device = IoDevice
      , batch_size = BatchSize
      , poke_freq = PokeFreq
    }
) ->
    case read(IoDevice, BatchSize) of
        {continue, Data} ->
            ydb_plan_node:notify(
                erlang:self()
              , {'$gen_cast', {delegate, {data, Data}}}
            )

          , timer:send_after(PokeFreq, {'$gen_cast', {delegate, Request}})

          , {ok, State}

      ; {done, Data} ->
            ydb_plan_node:notify(
                erlang:self()
              , {'$gen_cast', {delegate, {data, Data}}}
            )

          , file:close(IoDevice)

          , NewState = State#file_input{
                io_device=undefined
            }

          , {ok, NewState}
    end
;

delegate(_Request, State) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

init([], State = #file_input{}) ->
    post_init()

  , {ok, State}
;

init([{filename, Filename} | Options], State = #file_input{}) ->
    init(Options, State#file_input{io_device=open(Filename)})
;

init([{batch_size, BatchSize} | Options], State = #file_input{}) ->
    init(Options, State#file_input{batch_size=BatchSize})
;

init([{poke_freq, PollFreq} | Options], State = #file_input{}) ->
    init(Options, State#file_input{poke_freq=PollFreq})
;

init([Term | _Options], #file_input{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

post_init() -> gen_server:cast(erlang:self(), {delegate, {read}}).

%% ----------------------------------------------------------------- %%

open(Filename) ->
    {ok, IoDevice} = file:open(Filename, [read])
  , IoDevice
.

%% ----------------------------------------------------------------- %%

read(IoDevice, BatchSize) ->
    read(IoDevice, BatchSize, [])
.

read(_IoDevice, -1, Result) when is_list(Result) ->
    {done, lists:reverse(Result)}
;

read(_IoDevice, 0, Result) when is_list(Result) ->
    {continue, lists:reverse(Result)}
;

read(IoDevice, BatchSize, Result)
  when
    is_integer(BatchSize), BatchSize > 0
  , is_list(Result)
  ->
    case io:read(IoDevice, '') of
        {ok, Term} ->
            read(IoDevice, BatchSize - 1, [Term | Result])

      ; eof ->
            read(IoDevice, -1, Result)
    end
.

%% ----------------------------------------------------------------- %%
