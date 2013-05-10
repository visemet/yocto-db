%% @author Angela Gong, Kalpana Suraesh
%%         <anjoola@anjoola.com, ksuraesh@caltech.edu>

%% @doc This module contains utility functions used for enabling
%%      differentially-private queries.
-module(ydb_private_utils).

-export([get_next_power/1, get_prev_power/1, is_power_of_two/1]).
-export([store_bit_frequency/3, get_bit_frequency/2]).
-export([random_laplace/1]).


%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec store_bit_frequency(N :: integer(), V :: number(), Freqs :: dict()) ->
    NewFreqs :: dict()
.

%% @doc Updates the binary frequency table to store a frequency of
%%      V seen at time N. Returns the new table.
store_bit_frequency(N, V, Freqs) ->
    Max = get_max_key((dict:fetch_keys(Freqs)))
  , NewFreqs = lists:foldl(
        fun(X, Dict) -> store_bit_frequency(X, 0, Dict) end
      , Freqs
      , lists:seq(Max + 1, N - 1)) % returns [] if Max + 1 > N - 1

  , IntervalFreq = get_bit_frequency(N-1, NewFreqs)
                 - get_bit_frequency(N - storage_size(N), NewFreqs)
                 + V
  , dict:store(N, IntervalFreq, NewFreqs)
.

%% ----------------------------------------------------------------- %%

-spec get_bit_frequency(N :: integer(), Freqs :: dict())
    -> CumFreq :: number().

%% @ doc Returns cumulative frequency, calculated by recursively
%%       reading values from a binary frequency table.
get_bit_frequency(N, Freqs) ->
    get_bit_frequency(N, Freqs, 0)
.

%% ----------------------------------------------------------------- %%

-spec is_power_of_two(T :: integer()) -> IsPower :: boolean().

%% @doc Returns true if T is a power of 2.
is_power_of_two(T) ->
    T band (T - 1) == 0
.

%% ----------------------------------------------------------------- %%

-spec get_next_power(T :: integer()) -> NextPower :: integer().

%% @doc Returns the smallest power of 2 greater than T.
get_next_power(0) -> 1;

get_next_power(T) ->
    case is_power_of_two(T) of
        true -> 2 * T
      ; false -> get_next_power(T, T - 1, 1)
    end
.

%% ----------------------------------------------------------------- %%

-spec get_prev_power(T :: integer()) -> PrevPower :: integer().

%% @doc Returns the largest power of 2 less than or equal to T.
get_prev_power(T) ->
    get_next_power(T) bsr 1
.

%% ----------------------------------------------------------------- %%

-spec random_laplace(B :: float()) -> RandNum :: float().

%% @doc Draws a random number from the Lap(b) distribution. Draws are
%%      done by the following method, where F(x) is the cdf of
%%      the Laplacian distribution.
%%        1. Generate a random number *u* from uniform distribution
%%           in interval [0, 1].
%%        2. Compute the value x such that F(x) = u.
%%        3. Take x to be the random number drawn from the distribution
%%           (or we can just do x = F^(-1) (u) where F^(-1) is the
%%           inverse CDF).
%%      Lap(b) is the Laplace distribution with mean 0 and var 2b^2.
%%
%% http://en.wikipedia.org/wiki/Inverse_transform_sampling#The_method
%% http://en.wikipedia.org/wiki/Laplace_distribution#Cumulative_distribution_function
random_laplace(B) ->
    U = random:uniform()
  , RandNum = laplace_inverseCDF(U, B)
  , RandNum
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec get_bit_frequency(
    N :: integer()
  , Freqs :: dict()
  , CumFreq :: number()
)-> TotalCumFreq :: number().

%% @doc Tail-recursive method to calculate cumulative frequency
%%      from a binary frequency table.
get_bit_frequency(0, _, CumFreq) -> CumFreq;
get_bit_frequency(N, Freqs, CumFreq) ->
    case dict:find(N, Freqs) of
        {ok, V} ->
            get_bit_frequency(N - storage_size(N), Freqs, CumFreq + V)
      ; error ->
            get_bit_frequency(get_max_key(dict:fetch_keys(Freqs)), Freqs, 0)
    end
.

%% ----------------------------------------------------------------- %%

%% @doc Returns the number of values the node for this integer
%%      will store, based on the binary representation of the
%%      integer.
-spec storage_size(N :: integer()) -> Size :: integer().

storage_size(N) ->
    N band (bnot N + 1)
.

%% ----------------------------------------------------------------- %%

-spec get_max_key(Lst :: list()) -> Max :: integer().

%% @doc Returns the maximum of all the values in the list, or 0 if the
%%      list is empty.
get_max_key([]) -> 0;
get_max_key(Lst) ->
    lists:max(Lst)
.

%% ----------------------------------------------------------------- %%

-spec laplace_inverseCDF(U :: float(), B :: float()) -> InverseCDF :: float().

%% @doc Returns the number x such that F(x) = u, where F(x) is the
%%      CDF for the Laplace distribution with mean 0 and var 2b^2.
laplace_inverseCDF(U, B) ->
    X = -B * sgn(U - 0.5) * math:log(1 - 2 * abs(U - 0.5))
  , X
.

%% ----------------------------------------------------------------- %%

-spec sgn(Num :: number()) -> integer().

%% @doc Finds the sgn of a number.
sgn(Num) when Num < 0 -> -1;

sgn(Num) when Num == 0 -> 0;

sgn(Num) when Num > 0 -> 1.

%% ----------------------------------------------------------------- %%

-spec get_next_power(
    T :: integer()
  , NewT :: integer()
  , Shift :: integer()
) ->
    NextPower :: integer()
.

%% @doc Helper function for get_next_power/1
get_next_power(T, NewT, Shift) ->
    case is_power_of_two(NewT + 1) of
        true -> NewT + 1
      ; false -> get_next_power(T, NewT bor (NewT bsr Shift), Shift bsl 1)
    end
.