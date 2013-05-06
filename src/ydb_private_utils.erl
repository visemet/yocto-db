%% @author Angela Gong <anjoola@anjoola.com>
%%       , Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module contains utility functions used for enabling
%%      differentially-private queries.
-module(ydb_private_utils).

-export([random_laplace/1]).
-export([is_power_of_two/1, get_next_power/1, get_prev_power/1]).


%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec is_power_of_two(T :: integer()) -> IsPower :: boolean().

%% @doc returns true if T is a power of 2
is_power_of_two(T) ->
    T band (T - 1) == 0
.

%% ----------------------------------------------------------------- %%

-spec get_next_power(T :: integer()) -> NextPower :: integer().

%% @doc returns the smallest power of 2 greater than T
get_next_power(0) -> 1;
get_next_power(T) ->
    case is_power_of_two(T) of
        true -> 2 * T
      ; false -> get_next_power(T, T - 1, 1)
    end
.

%% ----------------------------------------------------------------- %%

-spec get_prev_power(T :: integer()) -> PrevPower :: integer().

%% @doc returns the largest power of 2 less than or equal to T
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