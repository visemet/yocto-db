%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc A generic publisher process.
-module(ydb_gen_pub).
-behaviour(gen_server).

%% interface functions
-export([
    start_link/3, start_link/4, subscribe/2, unsubscribe/2
]).

%% `gen_server' callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2
  , terminate/2, code_change/3
]).

%% for use only by extended modules
-export([init/2]).

%% @headerfile "ydb_gen_pub.hrl"
-include("ydb_gen_pub.hrl").

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-callback init(Args :: term()) ->
    {ok, State :: term()}
  | {error, Reason :: term()}
.

-callback compute_schema(
    InputSchemas :: [ydb_schema()]
  , State0 :: term()
) ->
    {ok, OutputSchema :: ydb_schema(), State1 :: term()}
  | {error, Reason :: term()}
.

%% ----------------------------------------------------------------- %%

-spec start_link(atom(), term(), list()) ->
    {ok, Publisher :: pid()}
  | {error, Reason :: term()}
.

%% @doc Starts the publisher in the supervisor hierarchy.
start_link(Type, Args, Options) ->
    gen_server:start_link(?MODULE, {Type, Args, Options}, [])
.

-spec start_link(atom(), atom(), term(), list()) ->
    {ok, Publisher :: pid()}
  | {error, Reason :: term()}
.

%% @doc Starts the publisher in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Type, Args, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, {Type, Args, Options}, [])
.

-spec subscribe(pid(), pid()) ->
    {ok, Schema :: ydb_schema()}
  | {error, 'already_subscribed'}
.

%% @doc Subscribes the process to the publisher.
subscribe(Publisher, Subscriber)
  when
    is_pid(Publisher)
  , is_pid(Subscriber)
  ->
    gen_server:call(Publisher, {subscribe, Subscriber})
.

-spec unsubscribe(pid(), pid()) -> ok.

%% @doc Unsubscribes the process from the publisher.
unsubscribe(Publisher, Subscriber)
  when
    is_pid(Publisher)
  , is_pid(Subscriber)
  ->
    gen_server:cast(Publisher, {unsubscribe, Subscriber})
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: {atom(), term(), list()}) ->
    {ok, State :: ydb_gen_pub()}
  | {stop, Reason :: term()}
.

%% @doc Initializes the internal state of the publisher.
init({Type, Args, Options}) ->
    State0 = #ydb_gen_pub{type=Type}

  , case init(Options, State0) of
        {ok, State1} ->
            case Type:init(Args) of
                {ok, Callback} ->
                    State2 = State1#ydb_gen_pub{callback=Callback}
                  , {ok, State2}

              ; {error, Reason} -> {stop, Reason}
            end

      ; {error, Reason} -> {stop, Reason}
    end
.

%% ----------------------------------------------------------------- %%

-spec handle_call(
    Request :: term()
  , From :: {pid(), Tag :: term()}
  , State0 :: ydb_gen_pub()
) ->
    {reply, Reply :: term(), State1 :: ydb_gen_pub()}
.

%% @doc Handles the requests to subscribe a process to the publisher.
handle_call(
    {subscribe, Subscriber}
  , _From
  , State0 = #ydb_gen_pub{
        schema = Schema
      , subscribers = Subscribers0
    }
) when
    is_pid(Subscriber)
  ->
    case get_ref(Subscriber, Subscribers0) of
        % `Subscriber' is not already a subscriber to the publisher
        undefined ->
            % Monitor `Subscriber' process
            Ref = erlang:monitor(process, Subscriber)

            % Add `{Subscriber, Ref}' to the set of subscribers
          , Subscribers1 = sets:add_element({Subscriber, Ref}, Subscribers0)

          , State1 = State0#ydb_gen_pub{subscribers=Subscribers1}
          , {reply, {ok, Schema}, State1}

        % `Subscriber' is already a subscriber to the publisher
      ; Ref when is_reference(Ref) ->
            {reply, {error, already_subscribed}, State0}
    end
;

handle_call(_Request, _From, State) -> {reply, ok, State}.

%% ----------------------------------------------------------------- %%

-spec handle_cast(Request :: term(), State0 :: ydb_gen_pub()) ->
    {noreply, State1 :: ydb_gen_pub()}
.

%% @doc Handles the requests to unsubscribe a process from the
%% publisher.
handle_cast(
    {unsubscribe, Subscriber}
  , State0 = #ydb_gen_pub{subscribers = Subscribers0}
) when
    is_pid(Subscriber)
  ->
    case get_ref(Subscriber, Subscribers0) of
        % `Subscriber' was not actually subscribed to the publisher
        undefined ->
            {noreply, State0}

        % `Subscriber' was actually subscribed to the publisher
      ; Ref when is_reference(Ref) ->
            % Remove `Subscriber' from the set of subscribers
            Subscribers1 = sets:del_element({Subscriber, Ref}, Subscribers0)

            % Unmonitor `Subscriber'
          , erlang:demonitor(Ref, [flush])

          , State1 = State0#ydb_gen_pub{subscribers=Subscribers1}
          , {noreply, State1}
    end
;

handle_cast(_Request, State) -> {noreply, State}.

%% ----------------------------------------------------------------- %%

-spec handle_info(Info :: timeout | term(), State0 :: ydb_gen_pub()) ->
    {noreply, State1 :: ydb_gen_pub()}
.

%% @doc Handles the info of down-ed subscribers.
handle_info(
    {'DOWN', Ref, process, Subscriber, _Reason}
  , State0 = #ydb_gen_pub{subscribers = Subscribers0}
) when
    is_reference(Ref)
  , is_pid(Subscriber)
  ->
    % Remove `Subscriber' from the set of subscribers
    Subscribers1 = sets:del_element({Subscriber, Ref}, Subscribers0)

    % Unmonitor `Subscriber'
  , erlang:demonitor(Ref, [flush])

  , State1 = State0#ydb_gen_pub{subscribers=Subscribers1}
  , {noreply, State1}
;

handle_info(_Info, State) -> {noreply, State}.

%% ----------------------------------------------------------------- %%

-spec terminate(term(), ydb_gen_pub()) -> ok.

%% @doc Called by a gen_server when it is about to terminate. Nothing
%%      to clean up though.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), State0 :: ydb_gen_pub(), term()) ->
    {ok, State1 :: ydb_gen_pub()}
.

%% @doc Called by a gen_server when it should update its interal state
%%      during a release upgrade or downgrade. Unsupported; the state
%%      remains the same.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%% =============================================================== %%%
%%%  internal functions                                             %%%
%%% =============================================================== %%%

-spec init(Options :: list(), State0 :: ydb_gen_pub()) ->
    {ok, State1 :: ydb_gen_pub()}
  | {error, {badarg, term()}}
.

%% @doc Initializes the internal state of the publisher.
init([], State) ->
    {ok, State}
;

init([Term | _Options], _State) ->
    {error, {badarg, Term}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec get_ref(pid(), set()) -> Ref :: 'undefined' | reference().

%% @doc Returns the reference associated with the subscriber.
get_ref(Subscriber, Subscribers) when is_pid(Subscriber) ->
    sets:fold(
        fun ({Pid, Ref}, undefined) when Pid =:= Subscriber -> Ref

          ; (_Item, undefined) -> undefined

          ; (_Item, Ref) when is_reference(Ref) -> Ref
        end

      , undefined
      , Subscribers
    )
.

%% ----------------------------------------------------------------- %%
