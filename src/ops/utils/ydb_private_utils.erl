%% @author Angela Gong, Kalpana Suraesh
%%         <anjoola@anjoola.com, ksuraesh@caltech.edu>

%% @doc This module contains utility functions used for enabling
%%      differentially-private queries.
-module(ydb_private_utils).

-export([do_bounded_advance/6, do_logarithmic_advance/5
       , do_bounded_sum_advance/6]).
-export([random_laplace/1]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec do_logarithmic_advance(
    CurrLState :: {number(), number()}
  , CurrTime :: integer()
  , NewTime :: integer()
  , Sigma :: number()
  , Eps :: number()
) -> {NewL :: number(), NewLT :: number()}.

%% @doc Adds as much noise as necessary and advances the value to
%%      incorporate the new value of Sigma.
%%
%%      Basically implements the following logic:
%%        if (NewTime &lt; get_next_power(CurrTime))
%%            {CurrL + Sigma, CurrLT}
%%        elseif is_power_of_two(NewTime)
%%            {add_noise(CurrL) + Sigma, add_noise(CurrL) + Sigma}
%%        else
%%            {add_noise(CurrL) + Sigma, add_noise(CurrL)}
do_logarithmic_advance(
    _CurrLState = {CurrL, CurrLT}
  , CurrTime
  , NewTime
  , Sigma
  , Eps
) ->
    case get_case(CurrTime, NewTime) of
        1 ->
            {CurrL + Sigma, CurrLT}
      ; 2 ->
            NumSteps = num_steps(CurrTime, NewTime)
          , NewLT = add_inveps_noise(CurrL, Eps, NumSteps) + Sigma
          , {NewLT, NewLT}
          % DEBUG: swap the previous line with the follwing:
          %, {CurrL + Sigma, CurrL + Sigma}
      ; 3 ->
            NumSteps = num_steps(CurrTime, NewTime)
          , NewLT = add_inveps_noise(CurrL, Eps, NumSteps)
          , {NewLT + Sigma, NewLT}
          % DEBUG: swap the previous line with the following
          %, {CurrL + Sigma, CurrL}
    end
.

%% ----------------------------------------------------------------- %%

-spec do_bounded_sum_advance(
    CurrMState :: {number(), dict()}
  , CurrTime :: integer()
  , NewTime :: integer()
  , Sigma :: number()
  , Eps :: number()
  , Mechanism :: 'binary'
) -> {NewM :: number()} | {NewM :: number(), NewFreqs :: dict()}.

%% @doc Advances the bounded mechanism to time NewTime with value
%%      Sigma, first resetting the state if necessary.
%%
%%      Implements the following logic:
%%        if (NewTime &lt; get_next_power(CurrTime))
%%            do regular advance
%%        elseif is_power_of_two(NewTime)
%%            reset mechanism state
%%        else
%%            reset mechanism state
%%            do advance with this (reset) state
do_bounded_sum_advance(undefined, C, N, S, E, 'binary') ->
    do_bounded_sum_advance({0, dict:new()}, C, N, S, E, 'binary')
;

do_bounded_sum_advance(CurrMState, CurrTime, NewTime, Sigma, Eps, 'binary') ->
    case get_case(CurrTime, NewTime) of
        1 -> do_binary_sum_advance(CurrMState, NewTime, Sigma, Eps)
      ; 2 -> {0, dict:new()}
      ; 3 -> do_binary_sum_advance({0, dict:new()}, NewTime, Sigma, Eps)
    end
.

%% ----------------------------------------------------------------- %%

-spec do_bounded_advance(
    CurrMState :: {number()} | {number(), dict()}
  , CurrTime :: integer()
  , NewTime :: integer()
  , Sigma :: number()
  , Eps :: number()
  , Mechanism :: 'binary' | 'simple_count_II'
) -> {NewM :: number()} | {NewM :: number(), NewFreqs :: dict()}.

%% @doc Advances the bounded mechanism to time NewTime with value
%%      Sigma, first resetting the state if necessary.
%%
%%      Implements the following logic:
%%        if (NewTime &lt; get_next_power(CurrTime))
%%            do regular advance
%%        elseif is_power_of_two(NewTime)
%%            reset mechanism state
%%        else
%%            reset mechanism state
%%            do advance with this (reset) state
do_bounded_advance(undefined, C, N, S, E, Mech) ->
    case Mech of
        'binary' -> do_bounded_advance({0, dict:new()}, C, N, S, E, Mech)
      ; 'simple_count_II' -> do_bounded_advance({0}, C, N, S, E, Mech)
    end
;

do_bounded_advance(CurrMState, CurrTime, NewTime, Sigma, Eps, 'binary') ->
    case get_case(CurrTime, NewTime) of
        1 -> do_binary_advance(CurrMState, NewTime, Sigma, Eps)
      ; 2 -> {0, dict:new()}
      ; 3 -> do_binary_advance({0, dict:new()}, NewTime, Sigma, Eps)
    end
;

do_bounded_advance(
    CurrMState, CurrTime, NewTime, Sigma, Eps, 'simple_count_II') ->
    case get_case(CurrTime, NewTime) of
        1 -> do_simple_count_II_advance(
                CurrMState, CurrTime, NewTime, Sigma, Eps)
      ; 2 -> {0}
      ; 3 -> do_simple_count_II_advance(
                {0}, get_next_power(CurrTime), NewTime, Sigma, Eps)
    end
.

%% ----------------------------------------------------------------- %%

-spec random_laplace(B :: float()) -> RandNum :: float().

%% @doc Draws a random number from the Lap(b) distribution. Draws are
%%      done by the following method, where F(x) is the cdf of
%%      the Laplace distribution.
%%      <ol>
%%        <li>Generate a random number <i>u</i> from uniform
%%            distribution in interval [0, 1].</li>
%%        <li>Compute the value x such that F(x) = <i>u</i>.</li>
%%        <li>Take x to be the random number drawn from the
%%            distribution (or we can just do x = F<sup>-1</sup>
%%            (<i>u</i>) where F<sup>-1</sup> is the inverse CDF.</li>
%%      </ol>
%%      Lap(b) is the Laplace distribution with mean 0 and variance
%%      2b<sup>2</sup>.
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

-spec do_binary_sum_advance(
    CurrMState :: {number(), dict()}
  , NewTime :: integer()
  , Sigma :: number()
  , Eps :: number()
) -> {NewM :: number(), NewFreqs :: dict()}.

%% @doc Adds a count of Sigma at time NewTime to the binary
%%      frequency table, and returns the new noisy count at NewTime as
%%      well as the new table. This is not pan-private, since the real
%%      values are stored in the frequency table.
do_binary_sum_advance(_State = {_CurrM, Freqs}, NewTime, Sigma, Eps) ->
    T = get_prev_power(NewTime)
  , Tau = NewTime - T
  , EpsPrime = Eps/(math:log(T)/math:log(2) + 1)
  , NewFreqs = store_bit_frequency(Tau, Sigma, Freqs, EpsPrime)
  , NewM = get_bit_frequency(Tau, NewFreqs)
  % DEBUG switch this line with the previous line to get the non-noisy
  % counts to make sure data is being calculated correctly.
  %, NewM = get_true_bit_frequency(Tau, NewFreqs)
  , {NewM, NewFreqs}
.

%% ----------------------------------------------------------------- %%

-spec do_binary_advance(
    CurrMState :: {number(), dict()}
  , NewTime :: integer()
  , Sigma :: number()
  , Eps :: number()
) -> {NewM :: number(), NewFreqs :: dict()}.

%% @doc Adds a count of Sigma at time NewTime to the binary
%%      frequency table, and returns the new noisy count at NewTime as
%%      well as the new table. This is not pan-private, since the real
%%      values are stored in the frequency table.
do_binary_advance(_State = {_CurrM, Freqs}, NewTime, Sigma, Eps) ->
    T = get_prev_power(NewTime)
  , Tau = NewTime - T
  , EpsPrime = Eps/(math:log(T)/math:log(2))
  , NewFreqs = store_bit_frequency(Tau, Sigma, Freqs, EpsPrime)
  , NewM = get_bit_frequency(Tau, NewFreqs)
  , {NewM, NewFreqs}
.

%% ----------------------------------------------------------------- %%

-spec do_simple_count_II_advance(
    CurrMState :: {number()}
  , CurrTime :: integer()
  , NewTime :: integer()
  , Sigma :: number()
  , Eps :: number()
) -> {NewM :: number()}.

%% @doc Adds a count of Sigma at time NewTime, as well as Lap(1/Eps)
%%      noise for each timestep between CurrTime and NewTime. This is
%%      pan private, as only the noisy count from each step is stored.
do_simple_count_II_advance(_State = {CurrM}, CurrTime, NewTime, Sigma, Eps) ->
    {add_inveps_noise(CurrM, Eps, NewTime - CurrTime) + Sigma}
.

%% ----------------------------------------------------------------- %%

-spec num_steps(CurrTime :: integer(), NewTime :: integer()) ->
    NumSteps :: integer().

%% @doc Calculates the number of powers of 2 that occur between
%%      CurrTime and NewTime.
num_steps(0, NewTime) ->
    round(math:log(get_prev_power(NewTime))/math:log(2)) + 1
;
num_steps(CurrTime, NewTime) ->
    round(math:log(get_prev_power(NewTime) / get_prev_power(CurrTime))
        / math:log(2))
.

%% ----------------------------------------------------------------- %%

-spec add_inveps_noise(
    Value :: number()
  , Eps :: number()
  , NumSteps :: integer()
) ->
    NoisyValue :: number()
.

%% @doc Adds Lap(1/Eps) noise to the specified value NumSteps times
%%      (i.e. returns Value + NumSteps * Lap(1/Eps)).
add_inveps_noise(Value, _Eps, 0) ->
    Value
;
add_inveps_noise(Value, Eps, NumSteps) ->
    NewValue = Value + random_laplace(1/Eps)
  , add_inveps_noise(NewValue, Eps, NumSteps - 1)
.

%% ----------------------------------------------------------------- %%

-spec get_case(CurrTime :: integer(), NewTime :: integer()) ->
    integer().

%% @doc Returns 1, 2, or 3, depending on what NewTime is in relation
%%      to CurrTime, and whether it's a power of 2.
%%
%%      Basically implements the following logic:
%%        if (NewTime &lt; get_next_power(CurrTime))
%%            1
%%        else if is_power_of_two(NewTime)
%%            2
%%        else
%%            3
get_case(CurrTime, NewTime) ->
    case NewTime < get_next_power(CurrTime) of
        true -> 1
      ; false ->
            case is_power_of_two(NewTime) of
                true -> 2
              ; false -> 3
            end
    end
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

%% ----------------------------------------------------------------- %%

-spec get_prev_power(T :: integer()) -> PrevPower :: integer().

%% @doc Returns the largest power of 2 less than or equal to T.
get_prev_power(T) ->
    get_next_power(T) bsr 1
.

%% ----------------------------------------------------------------- %%

-spec store_bit_frequency(
    N :: integer()
  , V :: number()
  , Freqs :: dict()
  , Eps :: number()
) ->
    NewFreqs :: dict()
.

%% @doc Updates the binary frequency table to store a frequency of
%%      V seen at time N. Returns the new table.
store_bit_frequency(N, V, Freqs, Eps) ->
    Max = get_max_key((dict:fetch_keys(Freqs)))
  , NewFreqs = lists:foldl(
        fun(X, Dict) -> store_bit_frequency(X, 0, Dict, Eps) end
      , Freqs
      , lists:seq(Max + 1, N - 1)) % returns [] if Max + 1 > N - 1

  , Alpha = get_true_bit_frequency(N-1, NewFreqs)
          - get_true_bit_frequency(N - storage_size(N), NewFreqs)
          + V
  , dict:store(N, {Alpha, add_inveps_noise(Alpha, Eps, 1)}, NewFreqs)
.

%% ----------------------------------------------------------------- %%

-spec get_bit_frequency(N :: integer(), Freqs :: dict()) ->
    CumFreq :: number().

%% @ doc Returns noisy cumulative frequency, calculated by recursively
%%       reading values from a binary frequency table.
get_bit_frequency(N, Freqs) ->
    get_bit_frequency(N, Freqs, 0, true)
.

%% ----------------------------------------------------------------- %%

-spec get_true_bit_frequency(N :: integer(), Freqs :: dict()) ->
    CumFreq :: number().

%% @ doc Returns cumulative frequency, calculated by recursively
%%       reading values from a binary frequency table.
get_true_bit_frequency(N, Freqs) ->
    get_bit_frequency(N, Freqs, 0, false)
.

%% ----------------------------------------------------------------- %%

-spec get_bit_frequency(
    N :: integer()
  , Freqs :: dict()
  , CumFreq :: number()
  , Noisy :: boolean()
)-> TotalCumFreq :: number().

%% @doc Tail-recursive method to calculate cumulative frequency
%%      from a binary frequency table. If Noisy is true, uses the
%%      noisy partial sums.
get_bit_frequency(0, _, CumFreq, _) -> CumFreq;
get_bit_frequency(N, Freqs, CumFreq, Noisy) ->
    case dict:find(N, Freqs) of
        {ok, {Alpha, AlphaHat}} ->
            if
                Noisy -> get_bit_frequency(
                    N - storage_size(N), Freqs, CumFreq + AlphaHat, Noisy)
              ; true -> get_bit_frequency(
                    N - storage_size(N), Freqs, CumFreq + Alpha, Noisy)
            end
      ; error -> 'undefined' %TODO should this throw an error
            %get_bit_frequency(
            %    get_max_key(dict:fetch_keys(Freqs)), Freqs, 0, Noisy)
    end
.


%% ----------------------------------------------------------------- %%

%% @doc Returns the number of values the node for this integer will
%%      store, based on the binary representation of the integer.
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
%%      CDF for the Laplace distribution with mean 0 and variance
%%      2b<sup>2</sup>.
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

