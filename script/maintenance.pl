:- module(
  maintenance,
  [
    compact/1, % +Base
    compact/2, % +Base, -File
    compile/0,
    compile/1, % +Base
    status/0,
    status/1,  % +Alias
    upload/1   % +Base
  ]
).

/** <module> LDFS maintenance

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(settings)).

:- use_module(library(call_ext)).
:- use_module(library(file_ext)).
:- use_module(library(rocks_ext)).
:- use_module(library(semweb/hdt_api)).
:- use_module(library(semweb/ldfs)).
:- use_module(library(tapir/tapir_api)).
:- use_module(library(thread_ext)).

alias_(seeds).
alias_(stale).
alias_(downloaded).
alias_(decompressed).
alias_(recoded).

:- at_halt(call_forall(alias_, rocks_close)).

:- initialization
   init_maintenance.





%! compact(+Base:oneof([data,error,meta,warning])) is det.
%! compact(+Base:oneof([data,error,meta,warning]), -File:atom) is det.

compact(Base) :-
  compact(Base, _).


compact(Base, ToFile) :-
  must_be(oneof([data,error,meta,warning]), Base),
  setting(ldfs:data_directory, Dir),
  file_name_extension(Base, 'nq.gz', Local),
  directory_file_path(Dir, Local, ToFile),
  write_to_file(
    ToFile,
    {Local}/[Out]>>(
      forall(
        nquads_candidate_(Local, FromFile),
        read_from_file(FromFile, {Out}/[In]>>copy_stream_data(In, Out))
      )
    )
  ),
  format(user_output, "🐱\n", []).

nquads_candidate_(Local, File) :-
  ldfs_file('', true, _, _, Local, File),
  \+ is_empty_file(File).



%! compile is det.
%! compile(+Base:oneof([data,error,meta,warning])) is det.

compile :-
  threaded_maplist_1(compile, [data,error,meta,warning]).


compile(Base) :-
  must_be(oneof([data,error,meta,warning]), Base),
  file_name_extension(Base, 'nq.gz', Local),
  forall(
    hdt_candidate_(Local, File),
    hdt_create(File)
  ).

hdt_candidate_(Local, File1) :-
  nquads_candidate_(Local, File1),
  hdt_file_name(File1, File2),
  \+ exists_file(File2).



%! status is det.
%! status(+Alias:oneof([downloaded,decompressed,recoded,seeds,stale]) is det.

status :-
  call_forall(alias_, status).

status(Alias) :-
  call_must_be(alias_, Alias),
  rocks_size(Alias, N),
  format("~a: ~D\n", [Alias,N]).



%! upload(+Base:oneof([data,meta])) is det.

upload(meta) :-
  maplist(compact, [error,meta], Files),
  Properties = _{accessLevel: public, files: Files, imports: [index]},
  dataset_upload(_, _, meta, Properties),
  maplist(delete_file, Files).





% INITIALIZATION

init_maintenance :-
  call_forall(
    alias_,
    [Alias]>>rocks_init(Alias, [key(atom),mode(read_only),value(term)])
  ).
