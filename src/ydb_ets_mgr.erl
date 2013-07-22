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
    {ready, NodeId}
  , Pid
  , State0 = #ets_mgr{
        topology = Topology
      , pids = Pids0
    }
) ->
    Pids1 = add_pid(NodeId, Pid, Pids0)

  , {_Length, Result0} = lists:foldl(
        fun (Elem, {Index0, Result0}) ->
            Index1 = Index0 + 1
          , Result1 = case Elem of
                {NodeId, From} ->
                    case dict:find(From, Pids1) of
                        {ok, Value} -> gen_server:cast(
                            Pid
                          , {publisher_pid, Value, From}
                        )

                      ; error -> pass
                    end

                  , [{Index1, From}|Result0]

              ; {To, NodeId} ->
                    case dict:find(To, Pids1) of
                        {ok, Value} -> gen_server:cast(
                            Value
                          , {publisher_pid, Pid, NodeId}
                        )

                      ; error -> pass
                    end

                  , Result0

              ; _Else -> Result0
            end

          , {Index1, Result1}
        end

      , {0, []}
      , Topology
    )

  , State1 = State0#ets_mgr{
        pids = Pids1
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

add_pid(NodeId, Pid, PidsDict) ->
    dict:store(NodeId, Pid, PidsDict)
.

%% ----------------------------------------------------------------- %%
