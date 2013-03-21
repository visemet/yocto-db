-module(file_input).
-behaviour(gen_server).

-export([start/3, do_read/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {file, batch_size, poke_freq}).

start(Name, Schema, Options) ->
    gen_server:start({local, Name}, ?MODULE, Options, [])
.

% do_read() -> gen_server:cast(erlang:self(), {read}).
do_read(Pid) -> gen_server:cast(Pid, {read}).

%% --------------------------------------------------------------------

init(Options) when is_list(Options) -> init(Options, #state{}).

init([], State = #state{}) ->
    post_init(State)
  , {ok, State}
;

init([{filename, Filename} | Options], State = #state{}) ->
    init(Options, State#state{file=open(Filename)})
;

init([{batch_size, BatchSize} | Options], State = #state{}) ->
    init(Options, State#state{batch_size=BatchSize})
;

init([{poke_freq, PollFreq} | Options], State = #state{}) ->
    init(Options, State#state{poke_freq=PollFreq})
.

handle_call(_Request, _From, State) -> {reply, ok, State}.

% handle_cast(_Request, State) -> {noreply, State}.

handle_cast(
    Request = {read}
  , State = #state{
        file = File
      , batch_size = BatchSize
      , poke_freq = PokeFreq
    }
) ->
    read(File, BatchSize)
  % , timer:send_after(PokeFreq, Request)

  , {noreply, State}
.

handle_info(_Info, State) -> {ok, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% --------------------------------------------------------------------

% post_init() -> gen_server:cast(erlang:self(), {read}).
post_init(State = #state{poke_freq = PokeFreq}) ->
    timer:apply_interval(
        PokeFreq
      , ?MODULE
      , do_read
      , [erlang:self()]
    )
.

open(Filename) ->
    file:open(Filename, [read])
.

read(IoDevice, BatchSize) ->
    io:format("Reading file...~n")
.

send(Data) ->
    gen_event:notify(erlang:self(), {data, Data})
.
