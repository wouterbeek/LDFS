:- module(
  ld_cli,
  [
    ld_dir/0,
    ld_dir/1,  % +Prefix
    ld_dir/2,  % +Prefix, +Options
    ld_root/2, % +Base, ?Root
    ld_root/3  % +Prefix, +Base, ?Root
  ]
).

/** <module> Linked Data Command-line Interface (LD-CLI)

Works on top of Linked Data File System (LDFS).

@author Wouter Beek
@version 2018
*/

:- use_module(library(ansi_term)).
:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(lists)).
:- use_module(library(ordsets)).
:- use_module(library(solution_sequences)).

:- use_module(library(dict)).
:- use_module(library(file_ext)).
:- use_module(library(pagination)).
:- use_module(library(semweb/hdt_api)).
:- use_module(library(semweb/ld_print)).
:- use_module(library(semweb/ldfs)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).

:- initialization
   rdf_register_prefixes.

:- meta_predicate
    hdt_call(+, +, 1),
    pp_page(+, 1).

:- rdf_meta
   hdt_root(+, r),
   hdt_root(+, +, r),
   hdt_tree_triple(+, r, -),
   hdt_tree_triple(+, +, r, -),
   cli_root(+, r),
   cli_root(+, +, r).





%! ld_dir is det.
%! ld_dir(+Prefix:atom) is det.
%! ld_dir(+Prefix:atom, +Options:dict) is det.

ld_dir :-
  ld_dir('').


ld_dir(Prefix) :-
  ld_dir(Prefix, _{}).


ld_dir(Prefix, Options) :-
  must_be(atom, Prefix),
  dict_get(page_number, Options, 1, First),
  must_be(positive_integer, First),
  dict_get(page_size, Options, 10, PageSize),
  must_be(positive_integer, PageSize),
  Offset is (First-1) * PageSize,
  Counter = count(First),
  findnsols(PageSize, Dir, offset(Offset, ldfs_directory(Prefix, Dir)), Dirs),
  Counter = count(PageNumber),
  pp_page(
    _{page_number: PageNumber, page_size: PageSize, results: Dirs},
    pp_hash_directory(Prefix)
  ),
  NextPageNumber is PageNumber + 1,
  nb_setarg(1, Counter, NextPageNumber).



%! ld_root(+Base:oneof([data,meta]), +Root:rdf_nonliteral) is semidet.
%! ld_root(+Base:oneof([data,meta]), -Root:rdf_nonliteral) is nondet.
%! ld_root(+Prefix:atom, +Base:oneof([data,meta]), +Root:rdf_nonliteral) is semidet.
%! ld_root(+Prefix:atom, +Base:oneof([data,meta]), -Root:rdf_nonliteral) is nondet.

ld_root(Base, Root) :-
  ld_root('', Base, Root).


ld_root(Prefix, Base, Root) :-
  hdt_root(Prefix, Base, Root),
  hdt_tree_triples(Prefix, Base, Root, Triples),
  ld_print_triples(Triples).





% HELPERS %

%! hdt_call(+Prefix:atom, +Base:oneof([data,meta]), :Goal_1) .

hdt_call(Prefix, Base, Goal_1) :-
  member(Base, [data,meta]),
  file_name_extension(Base, hdt, Local),
  ldfs_file(Prefix, Local, File),
  setup_call_cleanup(
    hdt_open(File, Hdt),
    call(Goal_1, Hdt),
    hdt_close(Hdt)
  ).



%! hdt_root(+Prefix:atom, +Base:oneof([data,meta]), +Root:rdf_nonliteral) is semidet.
%! hdt_root(+Prefix:atom, +Base:oneof([data,meta]), -Root:rdf_nonliteral) is nondet.

hdt_root(Prefix, Base, Root) :-
  hdt_call(Prefix, Base, hdt_root_(Root)).
hdt_root_(Root, Hdt) :-
  hdt_term(Hdt, subject, Root),
  \+ hdt_triple(Hdt, _, _, Root).



%! hdt_tree_triple(+Prefix:atom, +Base:oneof([data,meta]), +Root:rdf_nonliteral, -Triple:rdf_triple) is nondet.

hdt_tree_triple(Prefix, Base, Root, Triple) :-
  hdt_call(Prefix, Base, hdt_tree_triple_(Root, Triple)).
hdt_tree_triple_(Root, Triple, Hdt) :-
  hdt_tree_triple_(Root, [Root], Triple, Hdt).
hdt_tree_triple_(S, Hist1, Triple, Hdt) :-
  rdf_is_subject(S),
  hdt_triple(Hdt, S, P, O),
  (   Triple = rdf(S,P,O)
  ;   rdf_is_subject(O),
      ord_add_element(Hist1, O, Hist2),
      hdt_tree_triple_(O, Hist2, Triple, Hdt)
  ).



%! hdt_tree_triples(+Prefix:atom, +Base:oneof([data,meta]), +Root:rdf_nonliteral, -Triples:ordset(rdf_triple)) is det.

hdt_tree_triples(Prefix, Base, Root, Triples) :-
  hdt_call(Prefix, Base, hdt_tree_triples_(Root, Triples)).
hdt_tree_triples_(Root, Triples, Hdt) :-
  aggregate_all(set(Triple), hdt_tree_triple_(Root, Triple, Hdt), Triples).



%! pp_directory(+Directory:atom) is det.
%
% Print the files in Dir.

pp_directory(Dir) :-
  forall(
    directory_file(Dir, File),
    ansi_format([fg(green)], "    ~a", [File])
  ).



%! pp_hash_directory(+Prefix:atom, +Directory:atom) is det.

pp_hash_directory(Prefix, Dir) :-
  ldfs_is_finished(Dir),
  format(current_output, "  ", []),
  ldfs_directory_hash(Dir, Hash),
  atom_concat(Prefix, Rest, Hash),
  ansi_format([fg(green)], "~a|", [Prefix]),
  ansi_format([fg(red)], "~a", [Rest]),
  ansi_format([fg(green)], ":  ", []),
  pp_directory(Dir),
  nl.



%! pp_page(+Page:dict, :Goal_1) is det.
%
% Prints a Page from a paginated sequence, using Goal_1 to print the
% individual results.
%
% Page representation:
%
% ```prolog
% _{
%   page_number: positive_integer,
%   page_size: positive_integer,
%   results: list(term),
% }
% ```

pp_page(Page, Goal_1) :-
  dict_get(results, Page, [], Results),
  length(Results, NumResults),
  maplist(Goal_1, Results),
  nl,
  Attrs = [fg(green)],
  (   Results == []
  ->  ansi_format(Attrs, "No results for query.~n", [])
  ;   Low is (Page.page_number-1) * Page.page_size + 1,
      singular_plural(NumResults, Word),
      High is (Page.page_number-1) * Page.page_size + NumResults,
      ansi_format(Attrs, "Showing ~a ~D -- ~D for query.~n", [Word,Low,High])
  ).

singular_plural(1, result) :- !.
singular_plural(_, results).
