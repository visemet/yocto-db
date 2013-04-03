-module(ydb_plan_node).
-behaviour(gen_server).

-export([start_link/3, notify/2, add_listener/2, remove_listener/2]).

-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2
  , terminate/2, code_change/3
]).

-include("ydb_plan_node.hrl").

-record(plan_node, {
    type :: atom()

  , schema=[] :: ydb_schema()
  , timestamp='$auto_timestamp' :: '$auto_timestamp' | {atom(), atom()}

  , wrapped :: term()

  , listeners=sets:new() :: set()
}).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-callback init(Options :: term()) ->
    {ok, State :: term()}
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

start_link(Type, Args, Options) ->
    gen_server:start_link(?MODULE, [Type, Args, Options], [])
.

notify(PlanNode, Message) when is_pid(PlanNode) ->
    gen_server:cast(PlanNode, {notify, Message})
.

add_listener(PlanNode, Subscriber)
  when
    is_pid(PlanNode)
  , is_pid(Subscriber)
  ->
    gen_server:call(PlanNode, {subscribe, Subscriber})
.

remove_listener(PlanNode, Subscriber)
  when
    is_pid(PlanNode)
  , is_pid(Subscriber)
  ->
    gen_server:cast(PlanNode, {unsubscribe, Subscriber})
.

%% ----------------------------------------------------------------- %%

init([Type, Args, Options]) ->
    erlang:process_flag(trap_exit, true)

  , case Type:init(Args) of
        {ok, Wrapped} ->
            State = #plan_node{
                type=Type
              , wrapped=Wrapped
            }

          , init(Options, State)

      ; {error, Reason} ->
            {error, Reason}
    end
.

%% ----------------------------------------------------------------- %%

handle_call(
    {subscribe, Subscriber}
  , _From
  , State = #plan_node{listeners = Listeners}
) when
    is_pid(Subscriber)
  , is_list(Listeners)
  ->
    case get_ref(Subscriber, Listeners) of
        % `Subscriber' is not already a listener
        undefined ->
            % Monitor `Subscriber'
            Ref = erlang:monitor(process, Subscriber)

            % Add `{Subscriber, Ref}' to the set of listeners
          , NewListeners = sets:add_element({Subscriber, Ref}, Listeners)

          , {reply, ok, State#plan_node{listeners=NewListeners}}

        % `Subscriber' is already a listener
      ; Ref when is_reference(Ref) ->
            {reply, {error, already_subscribed}, State}
    end
;

handle_call(_Request, _From, State) ->
    {reply, ok, State}
.

%% ----------------------------------------------------------------- %%

handle_cast(
    {notify, Message}
  , State = #plan_node{listeners = Listeners}
) when
    is_list(Listeners)
  ->
    lists:foreach(
        fun (Subscriber) when is_pid(Subscriber) ->
            Subscriber ! Message
        end

      , Listeners
    )

  , {noreply, State}
;

handle_cast(
    {unsubscribe, Subscriber}
  , State = #plan_node{listeners = Listeners}
) when
    is_pid(Subscriber)
  , is_list(Listeners)
  ->
    case get_ref(Subscriber, Listeners) of
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
    {delegate, Message}
  , State = #plan_node{type = Type, wrapped = Wrapped}
) ->
    {ok, NewWrapped} = Type:delegate(Message, Wrapped)

  , {noreply, State#plan_node{wrapped=NewWrapped}}
;

handle_cast(
    {delegate, Message, Extras}
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

handle_cast(_Request, State) ->
    {noreply, State}
.

%% ----------------------------------------------------------------- %%

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

handle_info(_Info, State) ->
    {noreply, State}
.

%% ----------------------------------------------------------------- %%

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

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

init([Term | _Options], #plan_node{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

get_ref(Subscriber, Listeners)
  when
    is_pid(Subscriber)
  , is_list(Listeners)
  ->
    sets:foldl(
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

      , Listeners
    )
.

%% ----------------------------------------------------------------- %%
