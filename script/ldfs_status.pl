:- module(
  ldfs_status,
  [
    statements/0,
    status/0,
    status/1      % +Type
  ]
).

/** <module> LDFS status overview

@author Wouter Beek
@version 2018
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).

:- use_module(library(call_ext)).
:- use_module(library(semweb/ldfs)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).





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
%! status(+Type:oneof(downloaded,decompressed,recoded,parsed]) is det.

status :-
  forall(
    type_(Type),
    status(Type)
  ).

status(Local) :-
  call_must_be(type_, Local),
  aggregate_all(count, unfinished_file(Local, _), N),
  format("~a: ~D\n", [Local,N]).

unfinished_file(Local, File) :-
  ldfs_directory('', false, Dir, _),
  directory_file_path(Dir, Local, File),
  exists_file(File).

type_(downloaded).
type_(decompressed).
type_(recoded).
type_(parsed).
