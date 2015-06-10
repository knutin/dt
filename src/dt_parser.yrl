
Nonterminals
root
postgres_format
postgres_format_opt_clause
.

Terminals int years months days hours minutes seconds.

Rootsymbol root.

root -> postgres_format : '$1'.


%% "1 year 2 months 3 days 4 hours 5 minutes 6 seconds"
postgres_format ->
    postgres_format_opt_clause     % years
        postgres_format_opt_clause % months
        postgres_format_opt_clause % hours
        postgres_format_opt_clause % days
        postgres_format_opt_clause % minutes
        postgres_format_opt_clause % seconds
        :
        {postgres_interval, lists:flatten(['$1', '$2', '$3', '$4', '$5', '$6'])}.

postgres_format_opt_clause -> int years   : {years  , value_of('$1')}.
postgres_format_opt_clause -> int months  : {months , value_of('$1')}.
postgres_format_opt_clause -> int days    : {days   , value_of('$1')}.
postgres_format_opt_clause -> int hours   : {hours  , value_of('$1')}.
postgres_format_opt_clause -> int minutes : {minutes, value_of('$1')}.
postgres_format_opt_clause -> int seconds : {seconds, value_of('$1')}.
postgres_format_opt_clause -> '$empty'    : [].



Erlang code.

value_of({_, _, V}) -> V.
