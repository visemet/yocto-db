%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module contains utility functions used for enabling
%%      differentially-private queries.
-module(ydb_private_utils).

-export([random_laplace/1]).

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


-spec laplace_inverseCDF(U :: float(), B :: float()) -> InverseCDF :: float().

%% @doc Returns the number x such that F(x) = u, where F(x) is the
%%      CDF for the Laplace distribution with mean 0 and var 2b^2.
laplace_inverseCDF(U, B) ->
    X = -B * sgn(U - 0.5) * math:log(1 - 2 * abs(U - 0.5))
  , X
.
    
    
-spec sgn(Num :: number()) -> integer().

%% @doc Finds the sgn of a number.
sgn(Num) when Num < 0 -> -1;

sgn(Num) when Num == 0 -> 0;

sgn(Num) when Num > 0 -> 1.

%% ----------------------------------------------------------------- %%
