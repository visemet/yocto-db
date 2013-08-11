%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc A manager for ETS tables. Coordinates the startup phase of
%%      bolt processes.
-module(ydb_ets_mgr).
-behaviour(gen_server).

%% interface functions
-export([ready/3, new_ets/4]).

%% `gen_server' callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2
  , terminate/2, code_change/3
]).

%% @headerfile "ydb_gen_bolt.hrl"
-include("ydb_gen_bolt.hrl").

%% @headerfile "ydb_gen_spout.hrl"
-include("ydb_gen_spout.hrl").

-record(ets_mgr, {
    topology :: topology()

  , pids=dict:new() :: dict() % ydb_bolt_id() -> pid()
  , tids=dict:new() :: dict() % ydb_bolt_id() -> [ets:tid()]
}).

-type ets_mgr() :: #ets_mgr{
    topology :: topology()

  , pids :: dict() % ydb_bolt_id() -> pid()
  , tids :: dict() % ydb_bolt_id() -> [ets:tid()]
}.
%% Internal ETS manager state.

-type topology() :: [{Publisher :: pub_id(), Subscriber :: pub_id()}].
%% Topology of `Publisher -> Subscriber' graph.

-type pub_id() :: ydb_bolt_id() | ydb_spout_id().
%% Identifier for publisher process.

-type ets_config() :: [{Name :: atom(), Options :: [term()]}].
%% Configuration for ETS tables.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec ready(pid(), ydb_bolt_id(), pid()) ->
    {ok, Publishers :: [{pub_id(), 'undefined' | pid()}]}
.

%% @doc Signals to the manager that the bolt process is ready. Assumes
%%      that the message is sent from the bolt process itself.
ready(Manager, BoltId, BoltPid)
  when
    is_pid(Manager)
  , is_pid(BoltPid)
  ->
    gen_server:call(Manager, {ready, BoltId, BoltPid})
.

-spec new_ets(pid(), ets_config(), ydb_bolt_id(), pid()) ->
    Tids :: [ets:tid()]
.

%% @doc Requests potentially multiple ETS tables from the manager.
new_ets(Manager, Config, BoltId, BoltPid)
  when
    is_pid(Manager)
  , is_list(Config)
  , is_pid(BoltPid)
  ->
    gen_server:call(Manager, {new_ets, Config, BoltId, BoltPid})
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: {topology()}) ->
    {ok, ets_mgr()}
  | {stop, Reason :: term()}
.

%% @doc Initializes the internal state of the manager process.
init({Topology}) when is_list(Topology) ->
    case check_topology(Topology) of
        ok ->
            State = #ets_mgr{topology=Topology}
          , {ok, State}

      ; {error, Reason} -> {stop, Reason}
    end
.

-spec handle_call(
    Request :: term()
  , From :: {pid(), Tag :: term()}
  , State0 :: ets_mgr()
) ->
    {reply, Reply :: term(), State1 :: ets_mgr()}
.

%% @doc Handles the requests from the bolt processes that signal they
%%      are ready. Also handles the requests from the bolt processes
%%      to create ETS tables.
handle_call(
    {ready, ReadyId, ReadyPid}
  , _From
  , State0 = #ets_mgr{
        topology = Topology
      , pids = Pids0
    }
) ->
    Pids1 = add_ready_pid(ReadyId, ReadyPid, Pids0)

  , Result0 = lists:foldl(
        fun (Elem, Result) ->
            case {Publisher, Subscriber} = Elem of
                {{spout, Id}, ReadyId} ->
                    [{Publisher, erlang:whereis(Id)}|Result]

              ; {{bolt, _}, ReadyId} ->
                    on_subscriber_ready(Publisher, ReadyPid, Pids1)
                  , [{Publisher, undefined}|Result]

              ; {ReadyId, {bolt, _}} ->
                    on_publisher_ready(Subscriber, ReadyId, ReadyPid, Pids1)
                  , Result

              ; _Else -> Result
            end
        end

      , []
      , Topology
    )

  , Result1 = lists:reverse(Result0)

  , State1 = State0#ets_mgr{pids=Pids1}
  , {reply, {ok, Result1}, State1}
;

handle_call(
    {new_ets, Config, BoltId, BoltPid}
  , _From
  , State0 = #ets_mgr{tids = Tids0}
) ->
    Tids = lists:map(
        fun ({Name, Options0}) ->
            Options1 = [{heir, erlang:self(), {}}|Options0]

          , Tid = ets:new(Name, Options1)
          , ets:give_away(Tid, BoltPid, {})

          , Tid
        end

      , Config
    )

  , Tids1 = dict:store(BoltId, Tids, Tids0)

  , State1 = State0#ets_mgr{tids=Tids1}
  , {reply, {ok, Tids}, State1}
;

handle_call(_Request, _From, State) -> {reply, ok, State}.

%% ----------------------------------------------------------------- %%

-spec handle_cast(Request :: term(), State0 :: ets_mgr()) ->
    {noreply, State1 :: ets_mgr()}
.

%% @doc Does nothing.
handle_cast(_Request, State) -> {noreply, State}.

-spec handle_info(Info :: timeout | term(), State0 :: ets_mgr()) ->
    {noreply, State1 :: ets_mgr()}
.

%% @doc Does nothing.
handle_info(_Info, State) -> {noreply, State}.

-spec terminate(Reason :: term(), State :: ets_mgr()) -> ok.

%% @doc Called by a gen_server when it is about to terminate. Nothing
%%      to clean up though.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), State0 :: ets_mgr(), term()) ->
    {ok, State1 :: ets_mgr()}
.

%% @doc Called by a gen_server when it should update its interal state
%%      during a release upgrade or downgrade. Unsupported; the state
%%      remains the same.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec check_topology(topology()) -> ok | {error, Reason :: term}.

%% @doc Verifies that the topology is not malformed. Only allows
%%      connections from a spout process to a bolt process, or a bolt
%%      process to another bolt process.
check_topology(Topology) ->
    BadArgs = lists:filter(
        fun (Elem) ->
            case Elem of
                {{spout, _}, {bolt, _}} -> false

              ; {{bolt, _}, {bolt, _}} -> false

              ; _Else -> true
            end
        end

      , Topology
    )

  , if  erlang:length(BadArgs) =:= 0 -> ok

      ; true -> {error, {badarg, BadArgs}}
    end
.

-spec add_ready_pid(ydb_bolt_id(), pid(), Pids0 :: dict()) -> Pids1 :: dict().

%% @doc Returns a new dictionary with the `ReadyId -> ReadyPid' entry
%%      included.
add_ready_pid(ReadyId, ReadyPid, Pids) ->
    dict:store(ReadyId, ReadyPid, Pids)
.

-spec on_subscriber_ready(ydb_bolt_id(), pid(), dict()) -> ok.

%% @doc Handles the case where the newly-ready bolt process is a
%%      subscriber in the topology case.
on_subscriber_ready(PublisherId, SubscriberPid, Pids) ->
    % Check if publisher is also ready
    case dict:find(PublisherId, Pids) of
        {ok, PublisherPid} ->
            Request = {publisher, PublisherId, PublisherPid}
          , gen_server:cast(SubscriberPid, Request)

      ; error -> ok
    end
.

-spec on_publisher_ready(ydb_bolt_id(), ydb_bolt_id(), pid(), dict()) -> ok.

%% @doc Handles the case where the newly-ready bolt process is a
%%      publisher in the topology case.
on_publisher_ready(SubscriberId, PublisherId, PublisherPid, Pids) ->
    % Check if subscriber is also ready
    case dict:find(SubscriberId, Pids) of
        {ok, SubscriberPid} ->
            Request = {publisher, PublisherId, PublisherPid}
          , gen_server:cast(SubscriberPid, Request)

      ; error -> ok
    end
.

%% ----------------------------------------------------------------- %%
