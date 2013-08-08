%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc A manager for ETS tables. Coordinates the startup phase of
%%      bolt processes.
-module(ydb_ets_mgr).
-behaviour(gen_server).

%% interface functions
-export([ready/2]).

%% `gen_server' callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2
  , terminate/2, code_change/3
]).

%% @headerfile "ydb_gen_bolt.hrl"
-include("ydb_gen_bolt.hrl").

-record(ets_mgr, {
    topology :: [{ydb_bolt_id(), ydb_bolt_id()}]

  , pids=dict:new() :: dict() % ydb_bolt_id() -> pid()
  , tids=dict:new() :: dict() % ydb_bolt_id() -> [tid()]
}).

-type ets_mgr() :: #ets_mgr{
    topology :: [{To :: ydb_bolt_id(), From :: ydb_bolt_id()}]

  , pids :: dict() % ydb_bolt_id() -> pid()
  , tids :: dict() % ydb_bolt_id() -> [tid()]
}.
%% Internal ETS manager state.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec ready(pid(), ydb_bolt_id()) ->
    {ok, Publishers :: [{ydb_bolt_id(), 'undefined' | pid()}]}
.

%% @doc Signals to the manager that the bolt process is ready. Assumes
%%      that the message is sent from the bolt process itself.
ready(Manager, BoltId) when is_pid(Manager) ->
    gen_server:call(Manager, {ready, BoltId})
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: {[{ydb_bolt_id(), ydb_bolt_id()}]}) -> {ok, ets_mgr()}.

%% @doc Initializes the internal state of the manager process.
init({Topology}) when is_list(Topology) ->
    State = #ets_mgr{topology=Topology}
  , {ok, State}
.

-spec handle_call(
    Request :: term()
  , From :: {pid(), Tag :: term()}
  , State0 :: ets_mgr()
) ->
    {reply, Reply :: term(), State1 :: ets_mgr()}
.

%% @doc Handles the requests from the bolt processes that signal they
%%      are ready.
handle_call(
    {ready, ReadyId}
  , ReadyPid
  , State0 = #ets_mgr{
        topology = Topology
      , pids = Pids0
    }
) ->
    Pids1 = add_ready_pid(ReadyId, ReadyPid, Pids0)

  , Result0 = lists:foldl(
        fun (Elem, Result) ->
            case Elem of
                {ReadyId, From} ->
                    on_to_is_ready(From, ReadyId, ReadyPid, Pids1)
                  , [{From, undefined}|Result]

              ; {To, ReadyId} ->
                    on_from_is_ready(To, ReadyId, ReadyPid, Pids1)
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

-spec add_ready_pid(ydb_bolt_id(), pid(), Pids0 :: dict()) -> Pids1 :: dict().

%% @doc Returns a new dictionary with the `ReadyId -> ReadyPid' entry
%%      included.
add_ready_pid(ReadyId, ReadyPid, Pids) ->
    dict:store(ReadyId, ReadyPid, Pids)
.

-spec on_to_is_ready(ydb_bolt_id(), ydb_bolt_id(), pid(), dict()) -> ok.

%% @doc Handles the case where the newly-ready bolt process is on the
%%      `To'-side of the topology.
on_to_is_ready(FromId, _ToId, ToPid, Pids) ->
    case dict:find(FromId, Pids) of
        {ok, FromPid} -> gen_server:cast(ToPid, {publisher, FromId, FromPid})

      ; error -> ok
    end
.

-spec on_from_is_ready(ydb_bolt_id(), ydb_bolt_id(), pid(), dict()) -> ok.

%% @doc Handles the case where the newly-ready bolt process is on the
%%      `From'-side of the topology.
on_from_is_ready(ToId, FromId, FromPid, Pids) ->
    case dict:find(ToId, Pids) of
        {ok, ToPid} -> gen_server:cast(ToPid, {publisher, FromId, FromPid})

      ; error -> ok
    end
.

%% ----------------------------------------------------------------- %%
