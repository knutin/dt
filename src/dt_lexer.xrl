Definitions.

Rules.


[0-9]+      : {token, {int, TokenLine, ?l2i(TokenChars)}}.
year        : {token, {years, TokenLine, TokenChars}}.
years       : {token, {years, TokenLine, TokenChars}}.
month       : {token, {months, TokenLine, TokenChars}}.
months      : {token, {months, TokenLine, TokenChars}}.
day         : {token, {days, TokenLine, TokenChars}}.
days        : {token, {days, TokenLine, TokenChars}}.
hour        : {token, {hours, TokenLine, TokenChars}}.
hours       : {token, {hours, TokenLine, TokenChars}}.
minute      : {token, {minutes, TokenLine, TokenChars}}.
minutes     : {token, {minutes, TokenLine, TokenChars}}.
second      : {token, {seconds, TokenLine, TokenChars}}.
seconds     : {token, {seconds, TokenLine, TokenChars}}.


\s          : skip_token.


Erlang code.

-define(l2i(L), list_to_integer(L)).
