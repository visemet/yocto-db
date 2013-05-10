%% @author Angela Gong, Kalpana Suraesh
%%         <anjoola@anjoola.com, ksuraesh@caltech.edu>

%% @doc This module contains utility functions used for enabling
%%      differentially-private queries.
-module(ydb_private_utils).

-export([get_next_power/1, get_prev_power/1, is_power_of_two/1]).
-export([add_inveps_noise/3, store_bit_frequency/4, get_bit_frequency/2,
        do_simple_count_II_advance/4, do_logarithmic_advance/4,
        do_binary_advance/5]).
-export([random_laplace/1]).

%%% TODO put this into a hrl file and remove from both
-record(mech_state, {
    time :: integer()
  , value=0 :: number()
}).

-type mech_state() :: #mech_state{
    time :: undefined | integer()
  , value :: number()
}.
%% Internal mechanism state.


%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec do_binary_advance(
    CurrM :: mech_state()
  , New :: mech_state()
  , Tm :: mech_state()
  , Freqs :: dict()
  , Eps :: number()
) -> {NewM :: mech_state(), NewFreqs :: dict()}.

%% @doc Advances the state of the bounded mechanism M to time TNew,
%%      in this case adding noise as specified by simple counting
%%      mechanism II.
do_binary_advance(
    _Curr = #mech_state{}
  , _New = #mech_state{time=TSigma} % Sigma is 1 or 0
  , _Tm = #mech_state{time=T} % the last power of 2 below TNew
  , _Freqs
  , _Eps
) when TSigma == T ->
    {#mech_state{time=0, value=0}, dict:new()}
;

do_binary_advance(
    _Curr = #mech_state{}
  , _New = #mech_state{time=TSigma, value=Sigma} % Sigma is 1 or 0
  , _Tm = #mech_state{time=T} % the last power of 2 below TNew
  , Freqs
  , Eps
) ->
    Tau = TSigma - T
  , EpsPrime = Eps/T
  , NewFreqs = store_bit_frequency(Tau, Sigma, Freqs, EpsPrime)
  , CumFreq = get_bit_frequency(Tau, NewFreqs)
  , {#mech_state{time=Tau, value=CumFreq}, NewFreqs}
.

%% ----------------------------------------------------------------- %%

-spec do_logarithmic_advance(
    CurrL :: mech_state()
  , CurrT :: mech_state()
  , New :: mech_state()
  , Eps :: number()
) -> {NewL :: mech_state(), NewT :: mech_state()}.

%% @doc Adds as much noise as necessary and advances the value to
%%      incorporate \sigma.
do_logarithmic_advance(
    _CurrL = #mech_state{time=T1, value=Beta}
  , CurrT = #mech_state{}
  , New=#mech_state{time=T2, value=Sigma}
  , Eps
) ->
    Gnp1 = get_next_power(T1)
  , NewT = #mech_state{time=Gnp1, value=add_inveps_noise(Beta, Eps)}
  , case find_case(Gnp1, T2) of
        1 -> {CurrT, #mech_state{time=T2, value=Beta+Sigma}}
      ; 2 -> {NewT#mech_state{value=NewT#mech_state.value + Sigma}
                , #mech_state{time=T2, value=NewT#mech_state.value + Sigma}}
      ; 3 -> {NewT, #mech_state{time=T2, value=NewT#mech_state.value + Sigma}}
      ; 4 -> do_logarithmic_advance(NewT, NewT, New, Eps)
    end
.

%% ----------------------------------------------------------------- %%

-spec do_simple_count_II_advance(
    Curr :: mech_state()
  , New :: mech_state()
  , Tm :: mech_state()
  , Eps :: number()
) -> NewM :: mech_state().

%% @doc Advances the state of the bounded mechanism M to time TNew,
%%      in this case adding noise as specified by simple counting
%%      mechanism II.
do_simple_count_II_advance(
    _Curr = #mech_state{}
  , _New = #mech_state{time=TSigma} % Sigma is 1 or 0
  , _Tm = #mech_state{time=T} % the last power of 2 below TNew
  , _Eps
) when TSigma == T ->
    #mech_state{time=0, value=0}
;

do_simple_count_II_advance(
    _Curr = #mech_state{time=T1, value=V1}
  , _New = #mech_state{time=TSigma, value=Sigma} % Sigma is 1 or 0
  , _Tm = #mech_state{time=T} % the last power of 2 below TNew
  , Eps
) ->
    %?TRACE("do adv with ~ncurr = ~p, ~nnew = ~p, ~nt = ~p~n", [Curr, New, Tm]),
    {T2, V2} = {TSigma - T, Sigma}
  %, ?TRACE("call abn with ~p, ~p, ~p~n", [V1, Eps, T2-T1])
  , #mech_state{time=T2, value=add_inveps_noise(V1, Eps, T2 - T1) + V2}
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

-spec find_case(NextPower :: integer(), T2 :: integer()) ->
    Case :: integer().

%% @doc Returns value based on relative position of T2 and the current
%%      logarithmic interval in consideration. This tells us how to
%%      update the L and T mech_states.
find_case(NextPower, T2) when T2 < NextPower->
    1 % T2 is in this interval.
;
find_case(NextPower, T2) when T2 == NextPower->
    2 % T2 at the border of this interval.
;
find_case(NextPower, T2) when T2 < 2 * NextPower ->
    3 % T2 is in next interval.
;
find_case(_NextPower, _T2) ->
    4 % T2 is past next interval.
.

%% ----------------------------------------------------------------- %%


-spec add_inveps_noise(Value :: number(), Eps :: number()) ->
    NoisyValue :: number()
.

%% @doc Adds Lap(1/Eps) noise to the specified value, returning
%%      Value + Lap(1/Eps).
add_inveps_noise(Value, Eps) ->
    add_inveps_noise(Value, Eps, 1)
.

%% ----------------------------------------------------------------- %%

-spec add_inveps_noise(
    Value :: number()
  , Eps :: number()
  , NumSteps :: integer()
) ->
    NoisyValue :: number()
.

%% @doc Adds Lap(1/Eps) noise to the specified value NumSteps times,
%%      i.e. returns Value + NumSteps * Lap(1/Eps).
add_inveps_noise(Value, _Eps, 0) ->
    Value
;
add_inveps_noise(Value, Eps, NumSteps) ->
    %?TRACE("add bin noise with ~p, ~p, ~p~n", [Value, Eps, NumSteps]),
    NewValue = Value + random_laplace(1/Eps)
  , add_inveps_noise(NewValue, Eps, NumSteps - 1)
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