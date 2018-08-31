:- encoding(utf8).
:- module(
  script,
  [
    reset/0
  ]
).

:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(yall)).

:- use_module(library(call_ext)).
:- use_module(library(rocks_ext)).
:- use_module(library(semweb/ldfs)).

alias_(downloaded).
alias_(decompressed).
alias_(recoded).

:- at_halt(call_forall(alias_, rocks_close)).

:- debug(reset).

:- initialization
   init_reset.





%! reset is det.
%! reset(+Hash:atom) is det.

reset :-
  forall(
    alias_(Alias),
    reset_(Alias)
  ),
  forall(
    ldfs_directory('', false, Dir, Hash),
    (
      delete_directory_and_contents(Dir),
      debug(reset, "[SSD] ~a", [Hash])
    )
  ).

reset_(Alias) :-
  forall(
    rocks_enum(Alias, Hash, State1),
    (
      _{interval: Interval, uri: Uri} :< State1,
      State2 = _{interval: Interval, processed: 0.0, uri: Uri},
      rocks_put(stale, Hash, State2),
      rocks_delete(Alias, Hash, State1),
      debug(reset, "[RocksDB] ~a: ~a → stale", [Hash,Alias])
    )
  ).





% INITIALIZATION

init_reset :-
  call_forall(
    alias_,
    [Alias]>>rocks_init(Alias, [key(atom),value(term)])
  ).
