:- module(
  ldfs,
  [
    ldfs/4,           % +Base, ?S, ?P, ?O
    ldfs/5,           % +Base, ?S, ?P, ?O, ?Prefix
    ldfs_directory/3, % +Prefix, ?Finished, ?Directory
    ldfs_directory/4, % +Prefix, ?Finished, ?Directory, -Hash
    ldfs_file/6,      % +Prefix, ?Finished, ?Directory, -Hash, ?Local, ?File
    ldfs_root/1       % ?Directory
  ]
).

/** <module> Linked Data File System (LDFS)

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(lists)).
:- use_module(library(settings)).
:- use_module(library(yall)).

:- use_module(library(call_ext)).
:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(semweb/hdt_api)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).
:- use_module(library(tapir/tapir_api)).
:- use_module(library(thread_ext)).

:- initialization
   init_ldfs.

:- rdf_register_prefix(ll).

:- rdf_meta
   ldfs(+, r, r, o),
   ldfs(+, r, r, o, +).

:- setting(ldfs:data_directory, any, _, "").





%! ldfs(+Base:oneof([data,error,meta,warning]), ?S, ?P, ?O) is nondet.
%! ldfs(+Base:oneof([data,error,meta,warning]), ?S, ?P, ?O, ?Prefix) is nondet.

ldfs(Base, S, P, O) :-
  ldfs(Base, S, P, O, '').


ldfs(Base, S, P, O, Prefix) :-
  file_name_extension(Base, hdt, Local),
  ldfs_file(Prefix, true, _, _, Local, File),
  hdt_call(File, {S,P,O}/[Hdt]>>hdt_triple(Hdt, S, P, O)).



%! ldfs_directory(+Prefix:atom, ?Finished:boolean, +Directory:atom) is semidet.
%! ldfs_directory(+Prefix:atom, ?Finished:boolean, -Directory:atom) is nondet.
%! ldfs_directory(+Prefix:atom, ?Finished:boolean, +Directory:atom, -Hash:atom) is semidet.
%! ldfs_directory(+Prefix:atom, ?Finished:boolean, -Directory:atom, -Hash:atom) is nondet.

ldfs_directory(Prefix, Fin, Dir) :-
  ldfs_directory(Prefix, Fin, Dir, _).


ldfs_directory('', Fin, Dir2, Hash) :- !,
  (   ground(Dir2)
  ->  finished_(Fin, Dir2),
      directory_file_path2(Dir1, Hash2, Dir2),
      directory_file_path2(Root, Hash1, Dir1),
      ldfs_root(Root),
      atom_concat(Hash1, Hash2, Hash)
  ;   ldfs_root(Root),
      directory_subdirectory(Root, Hash1, Dir1),
      directory_subdirectory(Dir1, Hash2, Dir2),
      finished_(Fin, Dir2),
      atom_concat(Hash1, Hash2, Hash)
  ),
  atom_length(Hash, 32).
ldfs_directory(Prefix, Fin, Dir2, Hash) :-
  atom_length(Prefix, PrefixLength),
  ldfs_root(Root),
  (   % Hash falls within the first two characters (outer directory).
      PrefixLength =< 2
  ->  atom_concat(Prefix, *, Wildcard0),
      append_directories(Root, Wildcard0, Wildcard),
      expand_file_name(Wildcard, Dir1s),
      member(Dir1, Dir1s),
      directory_file(Dir1, Hash1),
      directory_subdirectory(Dir1, Hash2, Dir2),
      finished_(Fin, Dir2),
      atom_concat(Hash1, Hash2, Hash)
  ;   % Hash goes past the first two characters (inner directory).
      atom_codes(Prefix, [H1,H2|T1]),
      atom_codes(Hash1, [H1,H2]),
      append(T1, [0'*], T2),
      atom_codes(Wildcard0, T2),
      append_directories([Root,Hash1,Wildcard0], Wildcard),
      expand_file_name(Wildcard, Dir2s),
      member(Dir2, Dir2s),
      directory_file_path2(_, Hash2, Dir2),
      atom_concat(Hash1, Hash2, Hash)
  ),
  atom_length(Hash, 32).

finished_(Fin, Dir) :-
  directory_file_path(Dir, finished, File),
  call_bool(exists_file(File), Fin).



%! ldfs_file(+Prefix:atom, ?Finished:boolean, +Directory:atom, -Hash:atom, +Local:atom, +File:atom) is semidet.
%! ldfs_file(+Prefix:atom, ?Finished:boolean, +Directory:atom, -Hash:atom, +Local:atom, -File:atom) is semidet.
%! ldfs_file(+Prefix:atom, ?Finished:boolean, +Directory:atom, -Hash:atom, -Local:atom, +File:atom) is semidet.
%! ldfs_file(+Prefix:atom, ?Finished:boolean, +Directory:atom, -Hash:atom, -Local:atom, -File:atom) is nondet.
%! ldfs_file(+Prefix:atom, ?Finished:boolean, -Directory:atom, -Hash:atom, +Local:atom, +File:atom) is nondet.
%! ldfs_file(+Prefix:atom, ?Finished:boolean, -Directory:atom, -Hash:atom, +Local:atom, -File:atom) is nondet.
%! ldfs_file(+Prefix:atom, ?Finished:boolean, -Directory:atom, -Hash:atom, -Local:atom, +File:atom) is nondet.
%! ldfs_file(+Prefix:atom, ?Finished:boolean, -Directory:atom, -Hash:atom, -Local:atom, -File:atom) is nondet.

ldfs_file(Prefix, Fin, Dir, Hash, Local, File) :-
  ground(File), !,
  exists_file(File),
  ldfs_directory(Prefix, Fin, Dir, Hash),
  directory_file_path(Dir, Local, File).
ldfs_file(Prefix, Fin, Dir, Hash, Local, File) :-
  ldfs_directory(Prefix, Fin, Dir, Hash),
  directory_file_path2(Dir, Local, File),
  exists_file(File).



%! ldfs_root(+Directory:atom) is semidet.
%! ldfs_root(-Directory:atom) is det.

ldfs_root(Root) :-
  setting(ldfs:data_directory, Root).





% INITIALIZATION

init_ldfs :-
  conf_json(Conf),
  directory_file_path(Conf.'data-directory', ll, Root),
  create_directory(Root),
  set_setting(ldfs:data_directory, Root).
