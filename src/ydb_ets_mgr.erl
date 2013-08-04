-module(ydb_ets_mgr).
-behaviour(gen_server).

-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2
  , terminate/2, code_change/3
]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

-record(ets_mgr, {
    topology :: [{ydb_node_id(), ydb_node_id()}]

  , pids=dict:new() :: dict() % ydb_node_id() -> pid()
  , tids=dict:new() :: dict() % ydb_node_id() -> [tid()]
}).

-type ets_mgr() :: #ets_mgr{
    topology :: [{To :: ydb_node_id(), From :: ydb_node_id()}]

  , pids :: dict() % ydb_node_id() -> pid()
  , tids :: dict() % ydb_node_id() -> [tid()]
}.
%% Internal ETS manager state.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec init({[{ydb_node_id(), ydb_node_id()}]}) ->
    {ok, ets_mgr()}
.

init({Topology}) when is_list(Topology) ->
    State = #ets_mgr{
        topology=Topology
    }

  , {ok, State}
.

handle_call(
    {ready, ReadyId}
  , ReadyPid
  , State0 = #ets_mgr{
        topology = Topology
      , pids = Pids0
    }
) ->
    Pids1 = add_ready_pid(ReadyId, ReadyPid, Pids0)

  , Result0 = ydb_lists_ext:foldli(
        fun (Index, Elem, Result0) ->
            case Elem of
                {ReadyId, From} ->
                    on_to_is_ready(From, ReadyId, ReadyPid, Pids1)
                  , [{Index, From}|Result0]

              ; {To, ReadyId} ->
                    on_from_is_ready(To, ReadyId, ReadyPid, Pids1)
                  , Result0

              ; _Else -> Result0
            end
        end

      , []
      , Topology
    )

  , State1 = State0#ets_mgr{
        pids=Pids1
    }

  , Result1 = lists:reverse(Result0)
  , {reply, {ok, Result1}, State1}
;

handle_call(_Request, _From, State) ->
    {reply, ok, State}
.

handle_cast(_Request, State) ->
    {noreply, State}
.

handle_info(_Info, State) ->
    {noreply, State}
.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ----------------------------------------------------------------- %%


%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

add_ready_pid(ReadyId, ReadyPid, PidsDict) ->
    dict:store(ReadyId, ReadyPid, PidsDict)
.

on_to_is_ready(FromId, _ToId, ToPid, PidsDict) ->
    case dict:find(FromId, PidsDict) of
        {ok, FromPid} -> gen_server:cast(
            ToPid
          , {publisher_pid, FromPid, FromId}
        )

      ; error -> ok
    end
.

on_from_is_ready(ToId, FromId, FromPid, PidsDict) ->
    case dict:find(ToId, PidsDict) of
        {ok, ToPid} -> gen_server:cast(
            ToPid
          , {publisher_pid, FromPid, FromId}
        )

      ; error -> ok
    end
.

%% ----------------------------------------------------------------- %%
