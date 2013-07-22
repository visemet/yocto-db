%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc A generic plan node. Several listeners (processes) can be
%%      added and will be notified for every tuple computed by the
%%      plan node.
-module(ydb_plan_node).
-behaviour(gen_server).

-export([
    start_link/3, start_link/4, notify/2, add_listener/2
  , remove_listener/2, relegate/2, relegate/3
]).

-export([send_tuples/2, send_diffs/2, free_diffs/2]).

-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2
  , terminate/2, code_change/3
]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

-record(plan_node, {
    type :: atom()

  , schema=[] :: ydb_schema()
  , timestamp='$auto_timestamp' :: ydb_timestamp()

  , wrapped :: term()

  , listeners=sets:new() :: set()
  , manager :: 'undefined' | pid()
  , publishers :: 'undefined'
                | [{
                        Index :: pos_integer()
                     , NodeId :: ydb_node_id() | pid()
                  }]
}).

-type plan_node() :: #plan_node{
    type :: atom()

  , schema :: ydb_schema()
  , timestamp :: ydb_timestamp()

  , wrapped :: term()

  , listeners :: set()
  , manager :: 'undefined' | pid()
  , publishers :: 'undefined'
                | [{Index :: pos_integer(), NodeId :: ydb_node_id()}]
}.
%% Internal plan node state.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-callback init(Options :: term()) ->
    {ok, State :: term()}
  | {error, Reason :: term()}
.

-callback compute_schema(
    InputSchemas :: [ydb_schema()]
  , State :: term()
) ->
    {ok, OutputSchema :: ydb_schema()}
  | {error, Reason :: term()}
.

-callback delegate(Message :: term(), State :: term()) ->
    {ok, NewState :: term()}
  | {error, Reason :: term()}
.

-callback delegate(
    Message :: term()
  , State :: term()
  , Extras :: [atom()])
  ->
    {ok, NewState :: term()}
  | {error, Reason :: term()}
.

%% ----------------------------------------------------------------- %%

-spec start_link(atom(), term(), list()) ->
    {ok, pid()}
  | {error, term()}
.

%% @doc Starts the plan node in the supervisor hierarchy.
start_link(Type, Args, Options) ->
    gen_server:start_link(?MODULE, {Type, Args, Options}, [])
.

-spec start_link(atom(), atom(), term(), list()) ->
    {ok, pid()}
  | {error, term()}
.

%% @doc Starts the plan node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Type, Args, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, {Type, Args, Options}, [])
.

-spec notify(pid(), term()) -> ok.

%% @doc Notifies the listeners of the plan node with the message.
notify(PlanNode, Message) when is_pid(PlanNode) ->
    gen_server:cast(PlanNode, {notify, Message})
.

%% ----------------------------------------------------------------- %%

-spec add_listener(pid() | atom(), pid()) ->
    {ok, ydb_schema()}
  | {error, {already_subscribed, ydb_schema()}}
.

%% @doc Adds the subscriber as a listener to the plan node.
add_listener(PlanNode, Subscriber)
  when
    is_pid(PlanNode)
  , is_pid(Subscriber)
  ->
    gen_server:call(PlanNode, {subscribe, Subscriber})
;

add_listener(PlanNodeName, Subscriber)
  when
    is_atom(PlanNodeName)
  , is_pid(Subscriber)
  ->
    case erlang:whereis(PlanNodeName) of
        undefined ->
            erlang:exit({noproc, {?MODULE, add_listener,
                [PlanNodeName, Subscriber]
            }})

      ; PlanNodePid when is_pid(PlanNodePid) ->
            add_listener(PlanNodePid, Subscriber)
    end
.

%% ----------------------------------------------------------------- %%

-spec remove_listener(pid(), pid()) -> ok.

%% @doc Removes the subscriber as a listener from the plan node.
remove_listener(PlanNode, Subscriber)
  when
    is_pid(PlanNode)
  , is_pid(Subscriber)
  ->
    gen_server:cast(PlanNode, {unsubscribe, Subscriber})
.

-spec relegate(pid(), term()) -> ok.

%% @doc Processes the message with the specific type of plan node.
relegate(PlanNode, Message)
  when
    is_pid(PlanNode) orelse is_atom(PlanNode)
  ->
    gen_server:cast(PlanNode, {relegate, Message})
.

-spec relegate(pid(), term(), [atom()]) -> ok.

%% @doc Process the message with the specific type of plan node,
%%      including the additional state information.
relegate(PlanNode, Message, Extras)
  when
    is_pid(PlanNode) orelse is_atom(PlanNode)
  , is_list(Extras)
  ->
    gen_server:cast(PlanNode, {relegate, Message, Extras})
.

%% ----------------------------------------------------------------- %%

-spec send_tuples(PlanNode :: pid(), Tuples :: [ydb_tuple()]) -> ok.

%% @doc Sends the listeners of the plan node with a
%%      `{tuples, Tuples}' message.
send_tuples(PlanNode, Tuples) ->
    notify(PlanNode, {'$gen_cast', {relegate, {tuples, Tuples}}})
.

-spec send_diffs(PlanNode :: pid(), Diffs :: [ets:tid()]) -> ok.

%% @doc Sends the listeners of the plan node with a
%%      `{diffs, Diffs}' message.
send_diffs(PlanNode, Diffs) ->
    notify(PlanNode, {'$gen_cast', {relegate, {diffs, Diffs}}})
.

%% ----------------------------------------------------------------- %%

-spec free_diffs(PlanNode :: pid(), Diffs :: [ets:tid()]) -> ok.

%% @doc Deletes the diffs owned by the plan node.
free_diffs(PlanNode, Diffs) ->
    gen_server:cast(PlanNode, {free_diffs, Diffs})
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: {atom(), term(), list()}) ->
    {ok, plan_node()}
  | {stop, term()}
.

%% @doc Initializes the internal state of the plan node.
init({Type, Args, Options}) ->
    erlang:process_flag(trap_exit, true)

  , case Type:init(Args) of
        {ok, Wrapped} ->
            State = #plan_node{
                type=Type
              , wrapped=Wrapped
            }

          , init(Options, State)

      ; {error, Reason} ->
            {stop, Reason}
    end
.

%% ----------------------------------------------------------------- %%

-spec handle_call(
    Request :: term()
  , From :: {pid(), Tag :: term()}
  , State :: plan_node()
) ->
    {reply, Reply :: term(), NewState :: plan_node()}
.

%% @doc Handles the request to subscribe a listener.
handle_call(
    {subscribe, Subscriber}
  , _From
  , State = #plan_node{
        schema = Schema
      , listeners = Listeners
    }
) when
    is_pid(Subscriber)
  ->
    case get_ref(Subscriber, sets:to_list(Listeners)) of
        % `Subscriber' is not already a listener
        undefined ->
            % Monitor `Subscriber'
            Ref = erlang:monitor(process, Subscriber)

            % Add `{Subscriber, Ref}' to the set of listeners
          , NewListeners = sets:add_element({Subscriber, Ref}, Listeners)

          , {
                reply
              , {ok, Schema}
              , State#plan_node{listeners=NewListeners}
            }

        % `Subscriber' is already a listener
      ; Ref when is_reference(Ref) ->
            {reply, {error, {already_subscribed, Schema}}, State}
    end
;

handle_call(_Request, _From, State) ->
    {reply, ok, State}
.

%% ----------------------------------------------------------------- %%

-spec handle_cast(Request :: term(), State :: plan_node()) ->
    {noreply, NewState :: plan_node()}
.

%% @doc Handles the request to notify listeners of a message. Handles
%%      the request to unsubscribe a listener. Delegates the message to
%%      the specific type of plan node, possibly with additional
%%      information of the state.
handle_cast(
    {notify, Message}
  , State = #plan_node{listeners = Listeners}
) ->
    lists:foreach(
        fun ({Subscriber, _Ref}) when is_pid(Subscriber) ->
            Subscriber ! Message
        end

      , sets:to_list(Listeners)
    )

  , {noreply, State}
;

handle_cast(
    {unsubscribe, Subscriber}
  , State = #plan_node{listeners = Listeners}
) when
    is_pid(Subscriber)
  ->
    case get_ref(Subscriber, sets:to_list(Listeners)) of
        % `Subscriber' is not a listener
        undefined ->
            {noreply, State}

        % `Subscriber' is a listener
      ; Ref when is_reference(Ref) ->
            % Remove `Subscriber' from the set of listeners
            NewListeners = sets:del_element({Subscriber, Ref}, Listeners)

            % Unmonitor `Subscriber'
          , erlang:demonitor(Ref, [flush])

          , {noreply, State#plan_node{listeners=NewListeners}}
    end
;

handle_cast(
    {relegate, Message}
  , State = #plan_node{type = Type, wrapped = Wrapped}
) ->
    {ok, NewWrapped} = Type:delegate(Message, Wrapped)

  , {noreply, State#plan_node{wrapped=NewWrapped}}
;

handle_cast(
    {relegate, Message, Extras}
  , State = #plan_node{
        type = Type
      , schema = Schema
      , timestamp = Timestamp
      , wrapped = Wrapped
    }
) when
    is_list(Extras)
  ->
    {ok, NewWrapped} = Type:delegate(
        Message
      , Wrapped
      , lists:map(
            fun
                (schema) -> Schema

              ; (timestamp) -> Timestamp
            end

          , Extras
        )
    )

  , {noreply, State#plan_node{wrapped=NewWrapped}}
;

handle_cast(
    {call_ready, MgrFun, NodeId}
  , State0
) ->
    MgrPid = MgrFun()

  , {ok, Publishers} = gen_server:call(MgrPid, {ready, NodeId})

  , State1 = State0#plan_node{
        manager=MgrPid
      , publishers=Publishers
    }

  , {noreply, State1}
;

handle_cast(
    {publisher_pid, PublisherPid, PublisherNodeId}
  , State0 = #plan_node{
        publishers = Publishers0
    }
) ->
    {Publishers1, Ready} = lists:mapfoldl(
        fun (P, R) when is_pid(P) -> {P, R}

          ; (P, R) ->
            case P of
                PublisherNodeId -> {PublisherPid, R}

              ; _Else -> {P, false}
            end
        end

      , true
      , Publishers0
    )

  , State1 = State0#plan_node{
        publishers=Publishers1
    }

  , case Ready of
        true ->
            Type = State0#plan_node.type
          , Wrapped = State0#plan_node.wrapped

          , case Type:compute_schema(
                lists:map(
                    fun ({_Index, PlanNode}) ->
                        case add_listener(PlanNode, erlang:self()) of
                            {ok, Schema} -> Schema

                          ; {error, {already_subscribed, Schema}} -> Schema
                        end
                    end

                  , Publishers1
                )

              , Wrapped
            ) of
                {ok, Schema} ->
                    State2 = State1#plan_node{
                        schema=Schema
                    }

                  , {noreply, State2}

              ; {error, Reason} -> {stop, Reason, State1}
            end

      ; false -> {noreply, State1}
    end
;

handle_cast(
    {prepare_schema, PlanNodes}
  , State = #plan_node{
        type = Type
      , wrapped = Wrapped
    }
) when
    is_list(PlanNodes)
  ->
    case Type:compute_schema(
        lists:map(
            fun (PlanNode) when is_pid(PlanNode) ->
                case add_listener(PlanNode, erlang:self()) of
                    {ok, Schema} -> Schema

                  ; {error, {already_subscribed, Schema}} -> Schema
                end

              ; (PlanNode) when is_atom(PlanNode) ->
                case add_listener(PlanNode, erlang:self()) of
                    {ok, Schema} -> Schema

                  ; {error, {already_subscribed, Schema}} -> Schema
                end

              ; (PlanNode) when is_function(PlanNode, 0) ->
                case add_listener(PlanNode(), erlang:self()) of
                    {ok, Schema} -> Schema

                  ; {error, {already_subscribed, Schema}} -> Schema
                end

              ; ({BranchNode, BranchType})
              when
                is_pid(BranchNode)
              , is_atom(BranchType)
              ->
                Result = ydb_branch_node:add_listener(
                    BranchNode
                  , BranchType
                  , erlang:self()
                )

              , case Result of
                    {ok, Schema} -> Schema

                  ; {error, already_subscribed} -> []
                end
            end

          , PlanNodes
        )

      , Wrapped
    ) of
        {ok, Schema} ->
            relegate(erlang:self(), {get_listenees, PlanNodes})
          , {noreply, State#plan_node{schema=Schema}}

      ; {error, Reason} ->
            {stop, Reason, State}
    end
;

handle_cast(
    {free_diffs, Diffs}
  , State = #plan_node{}
) when
    is_list(Diffs)
  ->
    lists:foreach(
        fun (Diff) ->
            ets:delete(Diff)
        end

      , Diffs
    )

  , {noreply, State}
;

handle_cast(_Request, State) ->
    {noreply, State}
.

%% ----------------------------------------------------------------- %%

-spec handle_info(Info :: timeout | term(), State :: plan_node()) ->
    {noreply, NewState :: plan_node()}
.

%% @doc Removes down subscribers as listeners. Passes info receieved
%%      to the specific type of plan node if it is not dealt with here.
handle_info(
    {'DOWN', Ref, process, Subscriber, _Reason}
  , State = #plan_node{listeners = Listeners}
) when
    is_reference(Ref)
  , is_pid(Subscriber)
  ->
    % Remove `Subscriber' from the set of listeners
    NewListeners = sets:del_element({Subscriber, Ref}, Listeners)

    % Unmonitor `Subscriber'
  , erlang:demonitor(Ref, [flush])

  , {noreply, State#plan_node{listeners=NewListeners}}
;

handle_info(Info, State = #plan_node{type = Type, wrapped = Wrapped}) ->
    {ok, NewWrapped} = Type:delegate({info, Info}, Wrapped)
  , {noreply, State#plan_node{wrapped=NewWrapped}}
.


%% ----------------------------------------------------------------- %%

-spec terminate(Reason :: term(), State :: plan_node()) -> ok.

%% @doc Called by a gen_server when it is about to terminate. Nothing
%%      to clean up though.
terminate(_Reason, _State) -> ok.

-spec code_change(
    OldVsn :: term()
  , State :: plan_node()
  , Extra :: term()
) ->
    {ok, NewState :: plan_node()}
.

%% @doc Called by a gen_server when it should update its interal state
%%      during a release upgrade or downgrade. Unsupported; the state
%%      remains the same.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init(Options :: list(), State :: plan_node()) ->
    {ok, NewState :: plan_node()}
  | {error, {badarg, term()}}
.

%% @doc Initializes the internal state of the plan node. Accepts the
%%      schema, as well as the column name and unit of the timestamp
%%      field.
init([], State = #plan_node{}) ->
    {ok, State}
;

init([{schema, Schema} | Options], State = #plan_node{})
  when
    is_list(Options)
  ->
    init(Options, State#plan_node{schema=Schema})
;

init([Timestamp = {Unit, Name} | Options], State = #plan_node{})
  when
    is_atom(Unit)
  , is_atom(Name)
  , is_list(Options)
  ->
    init(Options, State#plan_node{timestamp=Timestamp})
;

init([{ready_args, MgrFun, NodeId} | Options], State)
  when
    is_function(MgrFun, 0)
  ->
    gen_server:cast(erlang:self(), {call_ready, MgrFun, NodeId})

  , init(Options, State)
;

init([{listen, PlanNodes} | Options], State = #plan_node{})
  when
    is_list(PlanNodes)
  ->
    gen_server:cast(erlang:self(), {prepare_schema, PlanNodes})

  , init(Options, State)
;

init([Term | _Options], #plan_node{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec get_ref(pid(), [{pid(), reference()}]) ->
    'undefined'
  | reference()
.

%% @doc Returns the listener reference associated with the subscriber.
get_ref(Subscriber, Listeners)
  when
    is_pid(Subscriber)
  , is_list(Listeners)
  ->
    lists:foldl(
        fun
            ({Pid, Ref}, undefined)
              when
                is_pid(Pid), Pid =:= Subscriber
              , is_reference(Ref)
              ->
                Ref

          ; (_Item, undefined) -> undefined

          ; (_Item, Ref) when is_reference(Ref) -> Ref
        end

      , undefined
      , Listeners
    )
.

%% ----------------------------------------------------------------- %%
