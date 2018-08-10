:- module(
  ldfs_status,
  [
    statements/0,
    status/0,
    status/1,     % +Alias
    status_loop/1 % +Interval
  ]
).
:- reexport(library(semweb/ldfs)).

/** <module> LDFS status overview

@author Wouter Beek
@version 2018
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).

:- use_module(library(call_ext)).
:- use_module(library(rocks_ext)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).

:- at_halt(call_forall(alias_, rocks_close)).

:- initialization
   init_ldfs_status.





%! statements is det.

statements :-
  aggregate_all(
    sum(N),
    (
      rdf_prefix_member(P, [ll:quadruples,ll:triples]),
      ldfs(meta, _, P, Lit),
      rdf_literal_value(Lit, N)
    ),
    N
  ),
  format("~D\n", [N]).



%! status is det.
%! status(+Alias:oneof([downloaded,decompressed,recoded,seeds,stale]) is det.

status :-
  call_forall(alias_, status).

status(Alias) :-
  call_must_be(alias_, Alias),
  rocks_size(Alias, N),
  format("~a: ~D\n", [Alias,N]).

unfinished_file(Alias, File) :-
  ldfs_directory('', false, Dir, _),
  directory_file_path(Dir, Alias, File),
  exists_file(File).

alias_(seeds).
alias_(stale).
alias_(downloaded).
alias_(decompressed).
alias_(recoded).



%! status_loop(+Interval:positive_integer) is det.

status_loop(N) :-
  status,
  nl,
  sleep(N),
  status_loop(N).





% INITIALIZATION

init_ldfs_status :-
  call_forall(
    alias_,
    [Alias]>>rocks_init(Alias, [key(atom),mode(read_only),value(term)])
  ).
