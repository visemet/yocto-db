%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc A branch node. Several listeners (processes) can be added and
%%      will be notified for every tuple of a particular type computed
%%      by another plan node.
-module(ydb_branch_node).
-behaviour(gen_server).

-export([start_link/3, notify/3, add_listener/3, remove_listener/3]).

-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2
  , terminate/2, code_change/3
]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

-record(branch_node, {
    schemas=[] :: [{atom(), ydb_plan_node:ydb_schema()}]
  , timestamps=[] :: [{atom(), ydb_plan_node:ydb_timestamp()}]

  , listeners=[] :: [{atom(), set()}]
}).

-type branch_node() :: #branch_node{
    schemas :: [{atom(), ydb_plan_node:ydb_schema()}]
  , timestamps :: [{atom(), ydb_plan_node:ydb_timestamp()}]
  , listeners :: [{atom(), set()}]}.
%% Internal branch node state.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(
    pid() | atom()
  , [{atom(), ydb_plan_node:ydb_schema()}]
  , [{atom(), ydb_plan_node:ydb_timestamp()}]
) ->
    {'ok', pid()}
  | {'error', term()}
.

%% @doc Starts the branch node in the supervisor hierarchy.
start_link(PlanNode, Schemas, Timestamps) ->
    gen_server:start_link(?MODULE, {PlanNode, Schemas, Timestamps}, [])
.

-spec notify(pid(), atom(), term()) -> 'ok'.

%% @doc Notifies the listeners of the branch node for that particular
%%      type with the message.
notify(PlanNode, Type, Message)
  when
    is_pid(PlanNode)
  , is_atom(Type)
  ->
    gen_server:cast(PlanNode, {notify, Type, Message})
.

-spec add_listener(pid(), atom(), pid()) ->
    'ok'
  | {'error', 'already_subscribed'}.

%% @doc Adds the subscriber as a listener to the branch node for that
%%      particular type.
add_listener(PlanNode, Type, Subscriber)
  when
    is_pid(PlanNode)
  , is_atom(Type)
  , is_pid(Subscriber)
  ->
    gen_server:call(PlanNode, {subscribe, Type, Subscriber})
.

-spec remove_listener(pid(), atom(), pid()) -> 'ok'.

%% @doc Removes the subscriber as a listener from the branch node for
%%      that particular type.
remove_listener(PlanNode, Type, Subscriber)
  when
    is_pid(PlanNode)
  , is_atom(Type)
  , is_pid(Subscriber)
  ->
    gen_server:cast(PlanNode, {unsubscribe, Type, Subscriber})
.

%% ----------------------------------------------------------------- %%

-spec init(
    Args :: {
        pid() | atom()
      , [{atom(), ydb_plan_node:ydb_schema()}]
      , [{atom(), ydb_plan_node:ydb_timestamp()}]
    }
) ->
    {ok, branch_node()}
  | {stop, term()}
.

%% @doc Initializes the internal state of the plan node.
init({PlanNode, Schemas, Timestamps}) ->
    erlang:process_flag(trap_exit, true)

  , ydb_plan_node:add_listener(PlanNode, erlang:self())

  , State = #branch_node{
        schemas=Schemas
      , timestamps=Timestamps
    }

  , {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec handle_call(
    Request :: term()
  , From :: {pid(), Tag :: term()}
  , State :: branch_node()
) ->
    {reply, Reply :: term(), NewState :: branch_node()}
.

%% @doc Handles the request to subscribe a listener.
handle_call(
    {subscribe, Type, Subscriber}
  , _From
  , State = #branch_node{listeners = Listeners}
) when
    is_atom(Type)
  , is_pid(Subscriber)
  ->
    DictListeners = dict:from_list(Listeners)
  , TypeListeners = dict:fetch(Type, DictListeners)

  , case get_ref(Subscriber, sets:to_list(TypeListeners)) of
        % `Subscriber' is not already a listener
        undefined ->
            % Monitor `Subscriber'
            Ref = erlang:monitor(process, Subscriber)

            % Add `{Subscriber, Ref}' to the set of listeners
          , NewTypeListeners = sets:add_element(
                {Subscriber, Ref}
              , TypeListeners
            )

          , NewListeners = dict:to_list(
                dict:store(
                    Type
                  , NewTypeListeners
                  , DictListeners
                )
            )

          , {reply, ok, State#branch_node{listeners=NewListeners}}

        % `Subscriber' is already a listener
      ; Ref when is_reference(Ref) ->
            {reply, {error, already_subscribed}, State}
    end
;

handle_call(_Request, _From, State) ->
    {reply, ok, State}
.

%% ----------------------------------------------------------------- %%

-spec handle_cast(Request :: term(), State :: branch_node()) ->
    {noreply, NewState :: branch_node()}
.

%% @doc Handles the request to notify listeners of a message. Handles
%%      the request to unsubscribe a listener. Delegates the message to
%%      the specific type of plan node, possibly with additional
%%      information of the state.
handle_cast(
    {notify, Type, Message}
  , State = #branch_node{listeners = Listeners}
) when
    is_atom(Type)
  ->
    DictListeners = dict:from_list(Listeners)
  , TypeListeners = dict:fetch(Type, DictListeners)

  , lists:foreach(
        fun (Subscriber) when is_pid(Subscriber) ->
            Subscriber ! Message
        end

      , sets:to_list(TypeListeners)
    )

  , {noreply, State}
;

handle_cast(
    {unsubscribe, Type, Subscriber}
  , State = #branch_node{listeners = Listeners}
) when
    is_atom(Type)
  , is_pid(Subscriber)
  ->
    DictListeners = dict:from_list(Listeners)
  , TypeListeners = dict:fetch(Type, DictListeners)

  , case get_ref(Subscriber, sets:to_list(TypeListeners)) of
        % `Subscriber' is not a listener
        undefined ->
            {noreply, State}

        % `Subscriber' is a listener
      ; Ref when is_reference(Ref) ->
            % Remove `Subscriber' from the set of listeners
            NewTypeListeners = sets:del_element(
                {Subscriber, Ref}
              , TypeListeners
            )

          , NewListeners = dict:to_list(
                dict:store(
                    Type
                  , NewTypeListeners
                  , DictListeners
                )
            )

            % Unmonitor `Subscriber'
          , erlang:demonitor(Ref, [flush])

          , {noreply, State#branch_node{listeners=NewListeners}}
    end
;

handle_cast(_Request, State) ->
    {noreply, State}
.

%% ----------------------------------------------------------------- %%

-spec handle_info(Info :: timeout | term(), State :: branch_node()) ->
    {noreply, NewState :: branch_node()}
.

%% @doc Removes down subscribers as listeners. Passes info receieved
%%      to the specific type of plan node if it is not dealt with here.
handle_info(
    {'DOWN', Ref, process, Subscriber, _Reason}
  , State = #branch_node{listeners = Listeners}
) when
    is_reference(Ref)
  , is_pid(Subscriber)
  ->
    DictListeners = dict:from_list(Listeners)

  , NewListeners = dict:to_list(
        dict:map(
            fun (_Type, TypeListeners) ->
                % Remove `Subscriber' from the set of listeners
                sets:del_element(
                    {Subscriber, Ref}
                  , TypeListeners
                )
            end

          , DictListeners
        )
    )

    % Unmonitor `Subscriber'
  , erlang:demonitor(Ref, [flush])

  , {noreply, State#branch_node{listeners=NewListeners}}
;

handle_info(_Info, State) ->
    {noreply, State}
.

%% ----------------------------------------------------------------- %%

-spec terminate(Reason :: term(), State :: branch_node()) -> ok.

%% @doc Called by a gen_server when it is about to terminate. Nothing
%%      to clean up though.
terminate(_Reason, _State) -> ok.

-spec code_change(
    OldVsn :: term()
  , State :: branch_node()
  , Extra :: term()
) ->
    {ok, NewState :: branch_node()}
.

%% @doc Called by a gen_server when it should update its internal state
%%      during a release upgrade or downgrade. Unsupported; the state
%%      remains the same.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

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
