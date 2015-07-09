%% @doc: Datetime with Postgres semantics.
%% http://www.postgresql.org/docs/9.4/static/datatype-datetime.html
-module(dt).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([date_from_string/2
        ,datetime_from_string/2
        ,datetime_from_epoch_seconds/1
        ,interval_from_string/1
        ,to_string/2
        ,date_eq/2
        ,date_le/2
        ,datetime_eq/2
        ,datetime_le/2
        ]).

-export([date_trunc/2
        ,'+'/2
        ,'-'/2
        ]).

-export([serialize/1
        ,deserialize/1
        ,raw/1
        ]).

%% MACROS
-define(l2i(L), list_to_integer(L)).
-define(i2l(L), integer_to_list(L)).

-define(GREGORIAN_EPOCH, 62167219200).


%%
%% TYPES
%%

-type epoch_seconds() :: integer().

-type timestamp() :: {dt, epoch_seconds(), utc}.
-type interval() :: {interval, Months::integer(), Days::integer(), Seconds::float()}.

-type date_string() :: string().
-type datetime_string() :: string().
-type interval_string() :: string().
-type datestyle() :: {iso, mdy} | {iso, dmy} | {iso, ymd}.
-type format() :: string().

-export_type([date_string/0, datetime_string/0, interval_string/0]).
-export_type([timestamp/0, interval/0]).

%%
%% API
%%


-spec date_from_string(date_string(), datestyle()) -> timestamp().
date_from_string(String, {iso, mdy}) ->
    first_success(
      [fun () ->
               {ok, [Y, M, D]} =  parse(String, "YYYY-MM-DD"),
               datetime_to_timestamp({{Y, M, D}, {0, 0, 0, 0}})
       end]).


-spec datetime_from_string(datetime_string(), datestyle()) -> timestamp().
datetime_from_string(String, {iso, mdy}) ->
    first_success(
      [
       fun () ->
               {ok, [Y, M, D, H, Mi, S]} =  parse(String, "YYYY-MM-DD HH:MI:SS"),
               datetime_to_timestamp({{Y, M, D}, {H, Mi, S}})
       end,
       fun () ->
               {ok, [Y, M, D]} =  parse(String, "YYYY-MM-DD"),
               datetime_to_timestamp({{Y, M, D}, {0, 0, 0}})
       end,
       fun () ->
               {ok, [Y, M, D, H, Mi, S]} =  parse(String, "YYYYMMDDTHHMISS"),
               datetime_to_timestamp({{Y, M, D}, {H, Mi, S}})
       end,
       fun () ->
               {ok, [Epoch]} = parse(String, "epoch"),
               {dt, Epoch * 1000000, utc}
       end
      ]).

datetime_from_epoch_seconds(EpochSeconds) when is_integer(EpochSeconds) ->
    {dt, EpochSeconds*1000000, utc}.


-spec interval_from_string(interval_string()) -> interval().
interval_from_string(String) ->
    case parse_interval(String) of
        {postgres_interval, L} ->
            Y  = proplists:get_value(years, L, 0),
            M  = proplists:get_value(months, L, 0),
            D  = proplists:get_value(days, L, 0),
            H  = proplists:get_value(hours, L, 0),
            Mi = proplists:get_value(minutes, L, 0),
            S  = proplists:get_value(seconds, L, 0),

            %% TODO: Postgres semantics says to shift the fraction of
            %% months into days, same for days into seconds.
            Months = Y div 12 + M,
            Days = D,
            Seconds = H*60*60 + Mi*60 + S,
            {interval, Months, Days, Seconds}
    end.




-spec to_string(format(), timestamp()) -> string().
to_string(Format, Timestamp) ->
    DateTime = timestamp_to_datetime(Timestamp),
    lists:flatten(do_to_string(DateTime, Format)).



date_trunc("day", Timestamp) ->
    {YMD, _} = timestamp_to_datetime(Timestamp),
    datetime_to_timestamp({YMD, {0, 0, 0, 0}});
date_trunc("month", Timestamp) ->
    {{Y, M, _}, _} = timestamp_to_datetime(Timestamp),
    datetime_to_timestamp({{Y, M, 1}, {0, 0, 0, 0}}).


'+'({dt, T, utc}, {interval, _, _, _} = I) ->
    {dt, T + interval_to_seconds(I) * 1000000, utc}.

'-'({dt, T, utc}, {interval, _, _, _} = I) ->
    {dt, T - interval_to_seconds(I) * 1000000, utc}.


date_eq({dt, A, utc}, {dt, B, utc}) -> A =:= B.
date_le({dt, A, utc}, {dt, B, utc}) -> A =< B.

datetime_eq({dt, A, utc}, {dt, B, utc}) -> A =:= B.
datetime_le({dt, A, utc}, {dt, B, utc}) -> A =< B.



serialize({dt, N, utc}) ->
    <<N:64/little-integer>>.

deserialize(<<N:64/little-integer>>) ->
    {dt, N, utc}.

raw({dt, N, utc}) ->
    N.

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

do_to_string(DateTime, "epoch") ->
    {dt, T, utc} = datetime_to_timestamp(DateTime),
    [?i2l(T)];

do_to_string({{Y, M, D}, {H, Mi, S, _Ms}}, "iso8601") ->
    io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0BZ",
                  [Y, M, D, H, Mi, S]);

%% Separators
do_to_string(DateTime, [$- | Format]) -> [$- | do_to_string(DateTime, Format)];
do_to_string(DateTime, [$  | Format]) -> [$  | do_to_string(DateTime, Format)];
do_to_string(DateTime, [$: | Format]) -> [$: | do_to_string(DateTime, Format)];

do_to_string(_, []) ->
    [].


parse_interval(String) ->
    case dt_lexer:string(String) of
        {ok, Tokens, _Endline} ->
            case dt_parser:parse(Tokens) of
                {ok, ParseTree} ->
                    ParseTree
            end
    end.

interval_to_seconds({interval, M, D, S}) ->
    S +
        D*24*60*60 +
        M*30*24*60*60.



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
                 date_from_string("2015-01-01", {iso, mdy})),

    ?assertEqual({dt, 1420070400000000, utc},
                 datetime_from_epoch_seconds(1420070400)),

    ?assertEqual("2015-01-01T00:00:00Z",
                 to_string("iso8601", datetime_from_string("2015-01-01", {iso, mdy}))).

format_test() ->
    T = datetime_from_string("2015-01-01 00:00:00", {iso, mdy}),
    ?assertEqual("2015-01-01", to_string("YYYY-MM-DD", T)),
    ?assertEqual("2015-01-01T00:00:00Z", to_string("iso8601", T)).

date_trunc_test() ->
    DayStart = datetime_from_string("2015-01-02 00:00:00", {iso, mdy}),

    T = datetime_from_string("2015-01-02 01:02:03", {iso, mdy}),

    ?assertEqual("2015-01-02 00:00:00",
                 to_string("YYYY-MM-DD HH:MI:SS", date_trunc("day", T))),
    ?assertEqual(DayStart, date_trunc("day", T)),
    ?assertEqual("2015-01-01T00:00:00Z",
                 to_string("iso8601", date_trunc("month", T))).


serialization_test() ->
    T = datetime_from_string("2015-01-02 03:04:05", {iso, mdy}),
    ?assertEqual("1420167845000000", to_string("epoch", T)),
    ?assertEqual(<<1420167845000000:64/little-integer>>, serialize(T)),
    ?assertEqual(T, deserialize(serialize(T))).


interval_test() ->
    ?assertEqual({interval, 0, 90, 0}, interval_from_string("90 days")),
    ?assertEqual({interval, 0, 0, 60*60}, interval_from_string("1 hour")),
    ?assertEqual({interval, 0, 0, 60}, interval_from_string("1 minute")),
    ?assertEqual({interval, 0, 0, 1}, interval_from_string("1 second")),
    ?assertEqual({interval, 0, 0, 60*60+60}, interval_from_string("1 hour 1 minute")).


add_test() ->
    T = datetime_from_string("2015-01-01 00:00:00", {iso, mdy}),
    ?assertEqual(60*60, interval_to_seconds(interval_from_string("1 hour"))),
    ?assertEqual("2015-01-01T01:00:00Z",
                 to_string("iso8601", '+'(T, interval_from_string("1 hour")))),
    ?assertEqual("2015-01-02T00:00:00Z",
                 to_string("iso8601", '+'(T, interval_from_string("1 day")))),
    ?assertEqual("2015-04-01T00:00:00Z",
                 to_string("iso8601", '+'(T, interval_from_string("90 days")))).

subtract_test() ->
    T = datetime_from_string("2015-01-01 00:00:00", {iso, mdy}),
    ?assertEqual("2014-12-01T00:00:00Z",
                 to_string("iso8601", '-'(T, interval_from_string("31 days")))).
