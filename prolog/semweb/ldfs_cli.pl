:- module(
  ldfs_cli,
  [
    ld_dir/0,
    ld_dir/1,  % +Prefix
    ld_dir/2,  % +Prefix, +Options
    ld_file/2, % +Prefix, +Local
    ld_file/3, % +Prefix, +Local, +Options
    ld_meta/1, % +Prefix
    ld_root/2, % +Base, ?Root
    ld_root/3  % +Prefix, +Base, ?Root
  ]
).
:- reexport(library(semweb/ldfs)).

/** <module> Linked Data Command-line Interface (LD-CLI)

Works on top of Linked Data File System (LDFS).

All query results are scoped to a single HDT file.  Consequently, this
module does not give query results _across_ files.

---

@author Wouter Beek
@version 2018
*/

:- use_module(library(ansi_term)).
:- use_module(library(apply)).
:- use_module(library(error)).
:- use_module(library(lists)).
:- use_module(library(ordsets)).
:- use_module(library(solution_sequences)).
:- use_module(library(yall)).

:- use_module(library(dict)).
:- use_module(library(file_ext)).
:- use_module(library(semweb/hdt_api)).
:- use_module(library(semweb/ld_print)).
:- use_module(library(semweb/ldfs)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).

:- initialization
   rdf_register_prefixes,
   maplist(rdf_register_prefix, [
     error-'https://lodlaundromat.org/error/def/',
     graph-'https://lodlaundromat.org/graph/',
     http-'https://lodlaundromat.org/http/def/',
     id-'https://lodlaundromat.org/id/',
     ll
   ]).

:- meta_predicate
    hdt_call(+, +, 1),
    pagination(1, 1, +),
    pp_page(+, 1).

:- rdf_meta
   cli_root(+, r),
   cli_root(+, +, r),
   hdt_tree_triple(+, r, -),
   hdt_tree_triple(+, +, r, -),
   ld_root(+, r),
   ld_root(+, +, r).





%! ld_dir is det.
%! ld_dir(+Prefix:atom) is det.
%! ld_dir(+Prefix:atom, +Options:dict) is det.
%
% The following options are supported:
%
%   * finished(+boolean)
%
%     Default is `true'.
%
%   * Other options are passed to pagination/3.

ld_dir :-
  ld_dir('').


ld_dir(Prefix) :-
  ld_dir(Prefix, _{}).


ld_dir(Prefix, Options) :-
  must_be(atom, Prefix),
  dict_get(finished, Options, true, Finished),
  pagination(
    {Prefix,Finished}/[Dir]>>ldfs_directory(Prefix, Finished, Dir, _),
    pp_hash_directory(Prefix, Finished),
    Options
  ).



%! ld_file(+Prefix:atom, +Local:atom) is nondet.
%! ld_file(+Prefix:atom, +Local:atom, +Options:dict) is nondet.

ld_file(Prefix, Local) :-
  ld_file(Prefix, Local, _{}).


ld_file(Prefix, Local, Options) :-
  maplist(must_be(atom), [Prefix,Local]),
  pagination(
    {Prefix,Local}/[Line]>>(
      ldfs_file(Prefix, true, _, _, Local, File),
      file_line(File, Line)
    ),
    [Line]>>format("~s~n", [Line]),
    Options
  ).



%! ld_meta(+Prefix:atom) is nondet.

ld_meta(Prefix) :-
  hdt_call(Prefix, meta, ld_meta_).
ld_meta_(Hdt) :-
  findall(rdf(S,P,O), hdt_triple(Hdt, S, P, O), Triples),
  ld_print_triples(Triples).



%! ld_root(+Base:oneof([data,meta]), +Root:rdf_nonliteral) is semidet.
%! ld_root(+Base:oneof([data,meta]), -Root:rdf_nonliteral) is nondet.
%! ld_root(+Prefix:atom, +Base:oneof([data,meta]), +Root:rdf_nonliteral) is semidet.
%! ld_root(+Prefix:atom, +Base:oneof([data,meta]), -Root:rdf_nonliteral) is nondet.

ld_root(Base, Root) :-
  ld_root('', Base, Root).


ld_root(Prefix, Base, Root) :-
  hdt_call(Prefix, Base, ld_root_(Root)).
ld_root_(Root, Hdt) :-
  hdt_root_(Root, Hdt),
  hdt_tree_triples_(Root, Triples, Hdt),
  ld_print_triples(Triples).





% HELPERS %

%! hdt_call(+Prefix:atom, +Base:oneof([data,meta]), :Goal_1) .

hdt_call(Prefix, Base, Goal_1) :-
  must_be(oneof([data,meta]), Base),
  file_name_extension(Base, hdt, Local),
  ldfs_file(Prefix, true, _, _, Local, File),
  setup_call_cleanup(
    hdt_open(File, Hdt),
    call(Goal_1, Hdt),
    hdt_close(Hdt)
  ).



%! hdt_root_(+Root:rdf_nonliteral, +Hdt:blob) is semidet.
%! hdt_root_(-Root:rdf_nonliteral, +Hdt:blob) is nondet.

hdt_root_(Root, Hdt) :-
  hdt_term(Hdt, subject, Root),
  \+ hdt_triple(Hdt, _, _, Root).



%! hdt_tree_triple_(+Root:rdf_nonliteral, -Triple:rdf_triple, +Hdt:blob) is nondet.
%! hdt_tree_triple_(-Root:rdf_nonliteral, -Triple:rdf_triple, +Hdt:blob) is nondet.

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



%! hdt_tree_triples_(+Root:rdf_nonliteral, -Triples:ordset(rdf_triple), +Hdt:blob) is det.

hdt_tree_triples_(Root, Triples, Hdt) :-
  aggregate_all(set(Triple), hdt_tree_triple_(Root, Triple, Hdt), Triples).



%! pagination(:Match_1, :Display_1, +Options:dict) is det.

pagination(Match_1, Display_1, Options) :-
  dict_get(page_number, Options, 1, First),
  must_be(positive_integer, First),
  dict_get(page_size, Options, 10, PageSize),
  must_be(positive_integer, PageSize),
  Offset is (First-1) * PageSize,
  Counter = count(First),
  findnsols(PageSize, Templ, offset(Offset, call(Match_1, Templ)), Results),
  Counter = count(PageNumber),
  pp_page(
    _{page_number: PageNumber, page_size: PageSize, results: Results},
    Display_1
  ),
  NextPageNumber is PageNumber + 1,
  nb_setarg(1, Counter, NextPageNumber).



%! pp_directory(+Directory:atom) is det.
%
% Print the files in Dir.

pp_directory(Dir) :-
  forall(
    directory_file(Dir, File),
    ansi_format([fg(green)], "    ~a", [File])
  ).



%! pp_hash_directory(+Prefix:atom, +Finished:boolean, +Directory:atom) is det.

pp_hash_directory(Prefix, Fin, Dir) :-
  format(current_output, "  ", []),
  ldfs_directory(Prefix, Fin, Dir, Hash),
  atom_concat(Prefix, Postfix, Hash),
  ansi_format([fg(green)], "~a|", [Prefix]),
  ansi_format([fg(red)], "~a", [Postfix]),
  ansi_format([fg(green)], ":  ", []),
  pp_directory(Dir),
  nl.



%! pp_page(+Page:dict, :Display_1) is det.
%
% Prints a Page from a paginated sequence, using Display_1 to print the
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

pp_page(Page, Display_1) :-
  dict_get(results, Page, [], Results),
  length(Results, NumResults),
  maplist(Display_1, Results),
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
