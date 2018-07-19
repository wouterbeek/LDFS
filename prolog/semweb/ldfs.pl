:- module(
  ldfs,
  [
    ldfs_compile/1,        % +Base
    ldfs_data/3,           % ?S, ?P, ?O
    ldfs_data/4,           % ?S, ?P, ?O, ?Prefix
    ldfs_directory/1,      % -Directory
    ldfs_directory/2,      % +Prefix, -Directory
    ldfs_directory_hash/2, % ?Directory, ?Hash
    ldfs_is_finished/1,    % +Directory
    ldfs_file/2,           % ?Local, -File
    ldfs_file/3,           % +Prefix, ?Local, -File
    ldfs_file_line/3,      % +Prefix, +Local, -Line
    ldfs_meta/3,           % ?S, ?P, ?O
    ldfs_meta/4,           % ?S, ?P, ?O, ?Prefix
    ldfs_next/2,           % +Hash1, -Hash2
    ldfs_root/1            % ?Directory
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
:- use_module(library(readutil)).
:- use_module(library(settings)).
:- use_module(library(yall)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(semweb/hdt_api)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).
:- use_module(library(thread_ext)).

:- initialization
   init_ldfs.

:- maplist(rdf_register_prefix, [
     ll-'https://lodlaundromat.org/def/'
   ]).

:- rdf_meta
   ldfs_data(r, r, o),
   ldfs_data(r, r, o, +),
   ldfs_meta(r, r, o),
   ldfs_meta(r, r, o, +).

:- setting(ldfs:data_directory, any, _, "").





%! ldfs_compile(+Base:oneof([data,meta])) is det.

ldfs_compile(Base) :-
  must_be(oneof([data,meta]), Base),
  file_name_extension(Base, 'nq.gz', Local),
  aggregate_all(
    set(File1),
    (
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
  ldfs_is_finished(Dir),
  file_directory_name(Dir, Dir0),
  file_directory_name(Dir0, Root),
  ldfs_root(Root).
ldfs_directory(Dir) :-
  ldfs_root(Root),
  directory_subdirectory(Root, Dir0),
  directory_subdirectory(Dir0, Dir),
  ldfs_is_finished(Dir).



%! ldfs_directory(+Prefix:atom, +Directory:atom) is semidet.
%! ldfs_directory(+Prefix:atom, -Directory:atom) is nondet.

ldfs_directory('', Dir) :- !,
  ldfs_directory(Dir).
ldfs_directory(Prefix, Dir2) :-
  atom_length(Prefix, PrefixLength),
  ldfs_root(Root),
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



%! ldfs_directory_hash(+Directory:atom, +Hash:atom) is semidet.
%! ldfs_directory_hash(+Directory:atom, -Hash:atom) is semidet.
%! ldfs_directory_hash(-Directory:atom, +Hash:atom) is semidet.

ldfs_directory_hash(Dir, Hash) :-
  ground(Dir), !,
  directory_subdirectories(Dir, Segments),
  reverse(Segments, [Postfix,Prefix|_]),
  atom_concat(Prefix, Postfix, Hash).
ldfs_directory_hash(Dir, Hash) :-
  ground(Hash), !,
  ldfs_directory(Dir1),
  atom_codes(Hash, [H1,H2|T]),
  maplist(atom_codes, [Dir2,Dir3], [[H1,H2],T]),
  append_directories([Dir1,Dir2,Dir3], Dir).
ldfs_directory_hash(Dir, Hash) :-
  instantiation_error(args([Dir,Hash])).



%! ldfs_is_finished(+Directory:atom) is semidet.

ldfs_is_finished(Dir) :-
  directory_file_path(Dir, finished, File),
  exists_file(File).



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



%! ldfs_file_line(+Prefix:atom, +Local:atom, -Line:string) is nondet.

ldfs_file_line(Prefix, Local, Line) :-
  ldfs_file(Prefix, Local, File),
  call_stream_file(File, ldfs_file_line_(Line)).
ldfs_file_line_(Line, Out) :-
  repeat,
  read_line_to_string(Out, Line),
  (Line == end_of_file -> !, fail ; true).



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
  rdf_prefix_iri(graph:Hash2, Entry).
ldfs_next(Hash1, Hash2) :-
  ground(Hash2), !,
  ldfs_meta(Archive, ll:entry, _, Hash2),
  rdf_prefix_iri(graph:Hash1, Archive).
ldfs_next(Hash1, Hash2) :-
  instantiation_error(args([Hash1,Hash2])).



%! ldfs_root(+Directory:atom) is semidet.
%! ldfs_root(-Directory:atom) is det.

ldfs_root(Dir) :-
  setting(ldfs:data_directory, Dir0),
  directory_file_path(Dir0, ll, Dir).





% INITIALIZATION

init_ldfs :-
  conf_json(Conf),
  create_directory(Conf.'data-directory'),
  set_setting(ldfs:data_directory, Conf.'data-directory').
