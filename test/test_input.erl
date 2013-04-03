% @author Angela Gong
%
% @doc This module tests input into the YoctoDB system. It makes sure that input
%      is properly read and handled.
-module(test_input).
-include_lib("eunit/include/eunit.hrl").

% ---------------------------------------------------------------------
% @doc
%
% @spec 
reverse_test() -> ?assert([1] =:= lists:reverse([1])).
