%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc A generic bolt process.
-module(ydb_gen_bolt).
-extends(ydb_gen_pub).

%% inteface functions
-export([new_synopsis/1]).

%% overrides `gen_server' callbacks
-export([handle_cast/2]).

%% for use only by extended modules
-export([init/2]).

%% @headerfile "ydb_gen_pub.hrl"
-include("ydb_gen_pub.hrl").

%% @headerfile "ydb_gen_bolt.hrl"
-include("ydb_gen_bolt.hrl").

%% @headerfile "ydb_tuple.hrl"
-include("ydb_tuple.hrl").

-define(ETS_NAME, synopsis).
-define(ETS_NAME_STRING, erlang:atom_to_list(?ETS_NAME)).
-define(ETS_OPTIONS, [duplicate_bag, protected, {keypos, 1}]).

-record(gen_bolt, {
    id :: ydb_bolt_id()

  , manager :: pid()
  , publishers :: [{ydb_bolt_id(), 'undefined' | pid()}]
}).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-callback receive_synopsis(Tids :: [ets:tid()], State0 :: term()) ->
    State1 :: term()
.

-callback process_tuples(InTuples :: [ydb_tuple()], State0 :: term()) ->
    {ok, OutTuples :: [ydb_tuple()], State1 :: term()}
  | {error, Reason :: term()}
.

-callback send_tuples(Tuples :: [ydb_tuple()], Subscribers :: [pid()]) -> ok.

%% ----------------------------------------------------------------- %%

-spec new_synopsis(pos_integer()) -> ok.

new_synopsis(N) when is_integer(N), N > 0 ->
    gen_server:cast(erlang:self(), {new_synopsis, N})
.

%% ----------------------------------------------------------------- %%

-spec handle_cast(Request :: term(), State0 :: ydb_gen_pub()) ->
    {noreply, State1 :: ydb_gen_pub()}
.

%% @doc Handles the requests containing the process information of a
%% publisher.
handle_cast(
    {call_ready, MgrFun, BoltId}
  , State0
) ->
    MgrPid = MgrFun()

  , {ok, Publishers} = ydb_ets_mgr:ready(MgrPid, BoltId, erlang:self())
  , Extras = #gen_bolt{id=BoltId, manager=MgrPid, publishers=Publishers}

  , State1 = State0#ydb_gen_pub{extras=Extras}

    % Check if ready, in case only subscribed to spout processes
  , case is_ready(Publishers) of
        true -> on_ready(State1)

      ; false -> {noreply, State1}
    end
;

handle_cast(
    {publisher, PublisherId, PublisherPid}
  , State0 = #ydb_gen_pub{
        extras = Extras0 = #gen_bolt{publishers = Publishers0}
    }
) ->
    Publishers1 = update_publisher(PublisherId, PublisherPid, Publishers0)

  , Extras1 = Extras0#gen_bolt{publishers=Publishers1}
  , State1 = State0#ydb_gen_pub{extras=Extras1}

  , case is_ready(Publishers1) of
        true -> on_ready(State1)

      ; false -> {noreply, State1}
    end
;

handle_cast(
    {new_synopsis, N}
  , State0 = #ydb_gen_pub{
        type = Type
      , callback = Callback0
      , extras = #gen_bolt{
            id = BoltId
          , manager = Manager
        }
    }
) ->
    Config = make_ets_config(N)
  , Tids = ydb_ets_mgr:new_ets(Manager, Config, BoltId, erlang:self())

  , Callback1 = Type:receive_synopsis(Tids, Callback0)

  , State1 = State0#ydb_gen_pub{callback=Callback1}
  , {noreply, State1}
;

handle_cast(
    {tuples, InTuples}
  , State0 = #ydb_gen_pub{
        type = Type
      , callback = Callback0
      , subscribers = Subscribers
    }
) ->
    case Type:process_tuples(InTuples, Callback0) of
        {ok, OutTuples, Callback1} ->
            Type:send_tuples(OutTuples, Subscribers)

          , State1 = State0#ydb_gen_pub{callback=Callback1}
          , {noreply, State1}

      ; {error, Reason} -> {stop, Reason, State0}
    end
;

handle_cast(Request, State) -> ?BASE_MODULE:handle_cast(Request, State).

%%% =============================================================== %%%
%%%  internal functions                                             %%%
%%% =============================================================== %%%

-spec init(Options :: list(), State0 :: ydb_gen_pub()) ->
    {ok, State1 :: ydb_gen_pub()}
  | {error, {badarg, term()}}
.

%% @doc Initializes the internal state of the bolt.
init([{ready_args, MgrFun, BoltId} | Options], State)
  when
    is_function(MgrFun, 0)
  ->
    gen_server:cast(erlang:self(), {call_ready, MgrFun, BoltId})
  , init(Options, State)
;

init(Options, State) -> ?BASE_MODULE:init(Options, State).

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec update_publisher(
    ydb_bolt_id()
  , pid()
  , Publishers0 :: [{ydb_bolt_id(), 'undefined' | pid()}]
) ->
    Publishers1 :: [{ydb_bolt_id(), 'undefined' | pid()}]
.

%% @doc TODO
update_publisher(PublisherId, PublisherPid, Publishers)
  when
    is_pid(PublisherPid)
  , is_list(Publishers)
  ->
    lists:map(
        fun ({Id, _Else}) when Id =:= PublisherId -> {Id, PublisherPid}

          ; (Elem) -> Elem
        end

      , Publishers
    )
.

-spec is_ready([{ydb_bolt_id(), 'undefined' | pid()}]) -> boolean().

%% @doc TODO
is_ready(Publishers) when is_list(Publishers) ->
    lists:foldl(
        fun ({_Id, Pid}, true) when is_pid(Pid) -> true

          ; (_Elem, _Result) -> false
        end

      , true
      , Publishers
    )
.

-spec on_ready(State0 :: ydb_gen_pub()) ->
    {noreply, State1 :: ydb_gen_pub()}
  | {stop, Reason :: term(), State1 :: ydb_gen_pub()}
.

%% @doc TODO
on_ready(State0 = #ydb_gen_pub{
    type = Type
  , callback = Callback
  , extras = #gen_bolt{publishers = Publishers}
}) ->
    InputSchemas = lists:map(
        fun (Publisher) ->
            case ?BASE_MODULE:subscribe(Publisher, erlang:self()) of
                {ok, Schema} -> Schema

              ; {error, Reason} -> erlang:error(Reason)
            end
        end

      , Publishers
    )

  , case Type:compute_schema(InputSchemas, Callback) of
        {ok, Schema} ->
            State1 = State0#ydb_gen_pub{schema=Schema}
          , {noreply, State1}

      ; {error, Reason} -> {stop, Reason, State0}
    end
.

-spec make_ets_config(pos_integer()) ->
    Config :: [{Name :: atom(), Options :: [term()]}]
.

%% @doc TODO
make_ets_config(1) ->
    [{?ETS_NAME, ?ETS_OPTIONS}]
;

make_ets_config(N) ->
    lists:map(
        fun (I) ->
            {erlang:list_to_atom(?ETS_NAME_STRING ++ [I]), ?ETS_OPTIONS}
        end

      , lists:seq(1, N)
    )
.

%% ----------------------------------------------------------------- %%
