%% @doc: Datetime with Postgres semantics.
%% http://www.postgresql.org/docs/9.4/static/datatype-datetime.html
-module(dt).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([date_from_string/2, datetime_from_string/2]).

%% MACROS
-define(l2i(L), list_to_integer(L)).
-define(i2l(L), integer_to_list(L)).

-define(GREGORIAN_EPOCH, 62167219200).

%%
%% API
%%

date_from_string(String, {iso, mdy}) ->
    first_success(
      [fun () ->
               case parse(String, "YYYY-MM-DD") of
                   {ok, [Year, Month, Date]} ->
                       datetime_to_timestamp({{Year, Month, Date}, {0, 0, 0, 0}})
               end
       end]).


datetime_from_string(String, {iso, mdy}) ->
    first_success(
      [fun () ->
               case parse(String, "YYYY-MM-DD HH:MI:SS") of
                   {ok, [Year, Month, Date, Hour, Minute, Second]} ->
                       datetime_to_timestamp({{Year, Month, Date}, {Hour, Minute, Second}})
               end
       end,
       fun () ->
               case parse(String, "YYYYMMDDTHHMISS") of
                   {ok, [Year, Month, Date, Hour, Minute, Second]} ->
                       datetime_to_timestamp({{Year, Month, Date}, {Hour, Minute, Second}})
              end
       end,
       fun () ->
               case parse(String, "epoch") of
                   {ok, [Epoch]} ->
                       {dt, Epoch * 1000000, utc}
               end
       end]).



to_string(Timestamp, Format) ->
    DateTime = timestamp_to_datetime(Timestamp),
    lists:flatten(do_to_string(DateTime, Format)).



date_trunc("day", Timestamp) ->
    {YMD, _} = timestamp_to_datetime(Timestamp),
    datetime_to_timestamp({YMD, {0, 0, 0, 0}}).


serialize({timestamp, N, utc}) ->
    <<N:64/little-integer>>.

deserialize(<<N:64/little-integer>>) ->
    {timestamp, N, utc}.

%%
%% INTERNALS
%%

parse(String, Format) ->
    {ok, do_parse(String, Format)}.

do_parse([C1, C2, C3, C4 | String], [$Y, $Y, $Y, $Y | Format]) ->
    [?l2i([C1, C2, C3, C4]) | do_parse(String, Format)];

do_parse([M1, M2 | String], [$M, $M | Format]) ->
    [?l2i([M1, M2]) | do_parse(String, Format)];

do_parse([D1, D2 | String], [$D, $D | Format]) ->
    [?l2i([D1, D2]) | do_parse(String, Format)];

do_parse([H1, H2 | String], [$H, $H | Format]) ->
    [?l2i([H1, H2]) | do_parse(String, Format)];

do_parse([Mi1, Mi2 | String], [$M, $I | Format]) ->
    [?l2i([Mi1, Mi2]) | do_parse(String, Format)];

do_parse([S1, S2 | String], [$S, $S | Format]) ->
    [?l2i([S1, S2]) | do_parse(String, Format)];

do_parse(S, "epoch") ->
    [?l2i(S)];

%% Separators
do_parse([$T | String], [$T | Format]) -> do_parse(String, Format);
do_parse([$- | String], [$- | Format]) -> do_parse(String, Format);
do_parse([$: | String], [$: | Format]) -> do_parse(String, Format);
do_parse([$  | String], [$  | Format]) -> do_parse(String, Format);
do_parse([], []) -> [];
do_parse(_NotMatched, _Format) -> throw(parsing_error).



do_to_string({{Y, _, _}, _} = DateTime,"YYYY" ++ Format) ->
    [?i2l(Y) | do_to_string(DateTime, Format)];
do_to_string({{_, M, _}, _} = DateTime, "MM" ++ Format) ->
    [io_lib:format("~2..0w", [M]) | do_to_string(DateTime, Format)];
do_to_string({{_, _, D}, _} = DateTime, "DD" ++ Format) ->
    [io_lib:format("~2..0w", [D]) | do_to_string(DateTime, Format)];

do_to_string({_, {H, _, _, _}} = DateTime, "HH" ++ Format) ->
    [io_lib:format("~2..0w", [H]) | do_to_string(DateTime, Format)];
do_to_string({_, {_, Mi, _, _}} = DateTime, "MI" ++ Format) ->
    [io_lib:format("~2..0w", [Mi]) | do_to_string(DateTime, Format)];
do_to_string({_, {_, _, S, _}} = DateTime, "SS" ++ Format) ->
    [io_lib:format("~2..0w", [S]) | do_to_string(DateTime, Format)];

%% Separators
do_to_string(DateTime, [$- | Format]) -> [$- | do_to_string(DateTime, Format)];
do_to_string(DateTime, [$  | Format]) -> [$  | do_to_string(DateTime, Format)];
do_to_string(DateTime, [$: | Format]) -> [$: | do_to_string(DateTime, Format)];
    
do_to_string(_, []) ->
    [].





first_success([F | Fs]) ->
    try
        F()
    catch
        throw:parsing_error ->
            first_success(Fs);

        Class:Any ->
            error_logger:info_msg("crashed: ~p:~p~n~p~n",
                                  [Class, Any, erlang:get_stacktrace()]),
            first_success(Fs)
    end;
first_success([]) ->
    throw(giving_up).



timestamp_to_datetime({dt, Timestamp, utc}) ->
    MicroSecond = Timestamp rem 1000000,
    Seconds = ?GREGORIAN_EPOCH + (Timestamp div 1000000),
    {YMD, {Hour, Minute, Second}} =
        calendar:gregorian_seconds_to_datetime(Seconds),
    {YMD, {Hour, Minute, Second, MicroSecond}}.
    

datetime_to_timestamp({{Year, Month, Date}, {Hour, Minute, Second}}) ->
    datetime_to_timestamp({{Year, Month, Date}, {Hour, Minute, Second, 0}});

datetime_to_timestamp({{Year, Month, Date}, {Hour, Minute, Second, MicroSeconds}}) ->
    GregSeconds = calendar:datetime_to_gregorian_seconds(
                    {{Year, Month, Date}, {Hour, Minute, Second}}),
    MicroSeconds = 0,
    Timestamp = ((GregSeconds - ?GREGORIAN_EPOCH) * 1000000)
        + MicroSeconds,
    {dt, Timestamp, utc}.
    




%%
%% TESTS
%%


parse_test() ->
    ?assertEqual({dt, 1420070400000000, utc},
                 datetime_from_string("2015-01-01 00:00:00", {iso, mdy})),

    ?assertEqual({dt, 1420070400000000, utc},
                 datetime_from_string("20150101T000000", {iso, mdy})),

    ?assertEqual({dt, 1420070400000000, utc},
                 datetime_from_string("1420070400", {iso, mdy})),

    ?assertEqual({dt, 1420070400000000, utc},
                 date_from_string("2015-01-01", {iso, mdy})).

format_test() ->
    T = datetime_from_string("2015-01-01 00:00:00", {iso, mdy}),
    ?assertEqual("2015-01-01",
                 to_string(T, "YYYY-MM-DD")).

date_trunc_test() ->
    DayStart = datetime_from_string("2015-01-01 00:00:00", {iso, mdy}),

    T = datetime_from_string("2015-01-01 01:02:03", {iso, mdy}),
    
    ?assertEqual("2015-01-01 00:00:00",
                 to_string(date_trunc("day", T), "YYYY-MM-DD HH:MI:SS")),

    ?assertEqual(DayStart, date_trunc("day", T)).
                 

