-module(file_input).
-behaviour(gen_server).

-export([start/3, do_read/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {fd, batch_size, poll_freq}).

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
    init(Options, State#state{fd=file:open(Filename, [read])})
;

init([{batch_size, BatchSize} | Options], State = #state{}) ->
    init(Options, State#state{batch_size=BatchSize})
;

init([{poll_freq, PollFreq} | Options], State = #state{}) ->
    init(Options, State#state{poll_freq=PollFreq})
.

handle_call(_Request, _From, State) -> {reply, ok, State}.

% handle_cast(_Request, State) -> {noreply, State}.

handle_cast(
    Request = {read}
  , State = #state{
        fd = Fd
      , batch_size = BatchSize
      , poll_freq = PollFreq
    }
) ->
    read(Fd, BatchSize)
  % , timer:send_after(PollFreq, Request)

  , {noreply, State}
.

handle_info(_Info, State) -> {ok, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% --------------------------------------------------------------------

% post_init() -> gen_server:cast(erlang:self(), {read}).
post_init(State = #state{poll_freq = PollFreq}) ->
    timer:apply_interval(
        PollFreq
      , ?MODULE
      , do_read
      , [erlang:self()]
    )
.

read(Fd, BatchSize) ->
    io:format("Reading file...~n")
.
