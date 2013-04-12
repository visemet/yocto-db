%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module contains utility functions used for sending tuples
%%      through a socket in an acceptable format.
-module(ydb_socket_utils).

-export([send_tuples/1, send_tuples/2, send_tuples/3]).

% Default port number.
-define(DEFAULT_PORT, 9001).
% Default address (localhost).
-define(DEFAULT_ADDR, {127, 0, 0, 1}).

-type address() ::
    inet:ip_address()
  | inet:hostname()
.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec send_tuples
    (PortNo :: integer(), Data :: list())
        -> ok | {error, {badarg, Term :: term()}}
  ; (Socket :: port(), Data :: list())
        -> ok | {error, {badarg, Term :: term()}}
  ; (Address :: address(), Term :: term())
        -> ok | {error, {badarg, Term :: term()}}
.

%% @doc Sends a list of tuples in binary format using the specified
%%      socket, port, or address.
send_tuples(PortNo, Data) when is_integer(PortNo) ->
    send_tuples(?DEFAULT_ADDR, PortNo, Data)
;

send_tuples(Sock, Data) when is_port(Sock) ->
    if (is_list(Data)) ->
        gen_tcp:send(Sock, term_to_binary(Data))
  ; true ->
        {error, {badarg, Data}}
    end
;

send_tuples(Address = {Block1, Block2, Block3, Block4}, Data)
  when
    is_integer(Block1)
  , is_integer(Block2)
  , is_integer(Block3)
  , is_integer(Block4)
  ->
    send_tuples(Address, ?DEFAULT_PORT, Data)
;

send_tuples(Address, Data) when is_atom(Address); is_list(Address) ->
    send_tuples(Address, ?DEFAULT_PORT, Data)
.

%% ----------------------------------------------------------------- %%

-spec send_tuples(Data :: list()) ->
    ok | {error, {badarg, Term :: term()}}
.

%% @doc Sends a list of tuples in binary format at the default port
%%      and address.
send_tuples(Data) -> send_tuples(?DEFAULT_ADDR, ?DEFAULT_PORT, Data).

%% ----------------------------------------------------------------- %%

-spec send_tuples(
    Address :: inet:ip_address() | inet:hostname()
  , PortNo :: integer()
  , Data :: list()
) ->
    ok | {error, {badarg, Term :: term()}}
.

%% @doc Creates a socket at the specified address and port and sends
%%      a list of tuples through that socket.
send_tuples(Address, PortNo, Data) ->
    {ok, Sock} = gen_tcp:connect(Address, PortNo, []),
    send_tuples(Sock, Data)
.

%% ----------------------------------------------------------------- %%
