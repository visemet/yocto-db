%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module contains utility functions used for sending
%%      tuples through a socket in an acceptable format.
-module(ydb_socket_utils).

%% Functions for the client
-export([send_tuples/2]).

% Default port number.
-define(DEFAULT_PORT, 9001).
% Default address (localhost).
-define(DEFAULT_ADDR, {127, 0, 0, 1}).

-spec send_tuples(Socket :: port(), Data :: list()) ->
    ok | {error, {badarg, Term :: term()}}
.

%% @doc Sends a list of tuples in binary format using
%%      the specified socket.
send_tuples(Sock, Data) when is_port(Sock) ->
    if (is_list(Data)) ->
        gen_tcp:send(Sock, term_to_binary(Data))
  ; true ->
        {error, {badarg, Data}}
    end
.

-spec create_con_and_send_tuples(
    Address :: inet:ip_address() | inet:hostname()
  , PortNo :: integer()
  , Data :: list()
) ->
    ok | {error, {badarg, Term :: term()}}
.

%% @doc Creates a socket at the specified address and port
%%      and sends a list of tuples  through that socket.
create_con_and_send_tuples(Address, PortNo, Data) ->
    {ok, Sock} = gen_tcp:connect(Address, PortNo, []),
    send_tuples(Sock, Data)
.

