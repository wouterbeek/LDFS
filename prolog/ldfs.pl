:- module(
  ldfs,
  [
    ldfs_compile/0,
    ldfs_data/3,      % ?S, ?P, ?O
    ldfs_data/4,      % ?S, ?P, ?O, ?Prefix
    ldfs_directory/1, % -Directory
    ldfs_directory/2, % +Prefix, -Directory
    ldfs_file/2,      % ?Local, -File
    ldfs_file/3,      % +Prefix, ?Local, -File
    ldfs_meta/3,      % ?S, ?P, ?O
    ldfs_meta/4,      % ?S, ?P, ?O, ?Prefix
    ldfs_next/2       % +Hash1, -Hash2
  ]
).

/** <module> Linked Data File System (LDFS)

@author Wouter Beek
@version 2018
*/

:- use_module(library(aggregate)).
:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(lists)).
:- use_module(library(settings)).
:- use_module(library(yall)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(sw/hdt_api)).
:- use_module(library(sw/rdf_prefix)).
:- use_module(library(sw/rdf_term)).
:- use_module(library(thread_ext)).

:- initialization
   init_ldfs.

:- maplist(rdf_assert_prefix, [
     ll-'https://lodlaundromat.org/def/'
   ]).

:- rdf_meta
   ldfs_data(r, r, o),
   ldfs_data(r, r, o, +),
   ldfs_meta(r, r, o),
   ldfs_meta(r, r, o, +).

:- setting(ldfs:data_directory, any, _, "").





%! ldfs_compile is det.

ldfs_compile :-
  aggregate_all(
    set(File1),
    (
      member(Local, ['data.nq.gz','meta.nq.gz']),
      ldfs_file(Local, File1),
      \+ is_empty_file(File1),
      hdt_file_name(File1, File2),
      \+ exists_file(File2)
    ),
    Files
  ),
  threaded_maplist(hdt_create, Files).



%! ldfs_data(?S, ?P, ?O) is nondet.
%! ldfs_data(?S, ?P, ?O, ?Prefix) is nondet.

ldfs_data(S, P, O) :-
  ldfs_data(S, P, O, '').


ldfs_data(S, P, O, Prefix) :-
  ldfs_file(Prefix, 'data.hdt', File),
  hdt_call(File, {S,P,O}/[Hdt]>>hdt_triple(Hdt, S, P, O)).



%! ldfs_directory(+Directory:atom) is semidet.
%! ldfs_directory(-Directory:atom) is nondet.

ldfs_directory(Dir) :-
  ground(Dir), !,
  is_finished_(Dir),
  file_directory_name(Dir, Dir0),
  file_directory_name(Dir0, Root),
  root_(Root).
ldfs_directory(Dir) :-
  root_(Root),
  directory_subdirectory(Root, Dir0),
  directory_subdirectory(Dir0, Dir),
  is_finished_(Dir).



%! ldfs_directory(+Prefix:atom, +Directory:atom) is semidet.
%! ldfs_directory(+Prefix:atom, -Directory:atom) is nondet.

ldfs_directory('', Dir) :- !,
  ldfs_directory(Dir).
ldfs_directory(Prefix, Dir2) :-
  atom_length(Prefix, PrefixLength),
  root_(Root),
  (   % Hash falls within the first two characters (outer directory).
      PrefixLength =< 2
  ->  atom_concat(Prefix, *, Wildcard0),
      append_directories(Root, Wildcard0, Wildcard),
      expand_file_name(Wildcard, Dir1s),
      member(Dir1, Dir1s),
      directory_subdirectory(Dir1, Dir2)
  ;   % Hash goes past the first two characters (inner directory).
      atom_codes(Prefix, [H1,H2|T1]),
      atom_codes(Dir1, [H1,H2]),
      append(T1, [0'*], T2),
      atom_codes(Wildcard0, T2),
      append_directories([Root,Dir1,Wildcard0], Wildcard),
      expand_file_name(Wildcard, Dir2s),
      member(Dir2, Dir2s)
  ).



%! ldfs_file(+Local:atom, +File:atom) is semidet.
%! ldfs_file(+Local:atom, -File:atom) is nondet.

ldfs_file(Local, File) :-
  ldfs_file('', Local, File).


%! ldfs_file(+Prefix:atom, +Local:atom, +File:atom) is semidet.
%! ldfs_file(+Prefix:atom, +Local:atom, -File:atom) is nondet.
%! ldfs_file(+Prefix:atom, -Local:atom, +File:atom) is semidet.
%! ldfs_file(+Prefix:atom, -Local:atom, -File:atom) is nondet.

ldfs_file(Prefix, Local, File) :-
  ground(File), !,
  exists_file(File),
  ldfs_directory(Prefix, Dir),
  directory_file_path(Dir, Local, File).
ldfs_file(Prefix, Local, File) :-
  ldfs_directory(Prefix, Dir),
  directory_file_path2(Dir, Local, File),
  exists_file(File).



%! ldfs_meta(?S, ?P, ?O) is nondet.
%! ldfs_meta(?S, ?P, ?O, ?Prefix) is nondet.

ldfs_meta(S, P, O) :-
  ldfs_meta(S, P, O, '').


ldfs_meta(S, P, O, Prefix) :-
  ldfs_file(Prefix, 'meta.hdt', File),
  hdt_call(File, {S,P,O}/[Hdt]>>hdt_triple(Hdt, S, P, O)).



%! ldfs_next(+Hash1:atom, -Hash2:atom) is nondet.

ldfs_next(Hash1, Hash2) :-
  ground(Hash1), !,
  ldfs_meta(_, ll:entry, Entry, Hash1),
  rdf_global_id(graph:Hash2, Entry).
ldfs_next(Hash1, Hash2) :-
  ground(Hash2), !,
  ldfs_meta(Archive, ll:entry, _, Hash2),
  rdf_global_id(graph:Hash1, Archive).
ldfs_next(Hash1, Hash2) :-
  instantiation_error(args([Hash1,Hash2])).





% HELPERS %

%! is_finished_(+Directory:atom) is semidet.

is_finished_(Dir) :-
  directory_file_path(Dir, finished, File),
  exists_file(File).



%! root_(+Root:atom) is semidet.
%! root_(-Root:atom) is det.

root_(Root) :-
  setting(ldfs:data_directory, Dir),
  directory_file_path(Dir, ll, Root).





% INITIALIZATION

init_ldfs :-
  conf_json(Conf),
  create_directory(Conf.'data-directory'),
  set_setting(ldfs:data_directory, Conf.'data-directory').
