:- module(
  ld_print,
  [
    ld_print_triples/1 % +Triples
  ]
).

/** <module> Linked Data printing

We start with a list of RDF triple-denoting compound terms of the form
rdf/3.

We transform those into the following structure:

```
struct(
  [S1-[P1-[O1,…],…],…], % IRI trees
  [B1-[P1-[O1,…],…],…], % blank node trees
  [B1-L1,…]             % blank node collections
```

*/

:- use_module(library(apply)).
:- use_module(library(lists)).

:- use_module(library(rdf_prefix)).
:- use_module(library(rdf_term)).

:- rdf_meta
   ld_print_collection(o, ?),
   ld_print_literal(o),
   ld_print_predicate(r),
   rdf_list_triple(t),
   triples_collections_(+, t, -).





%! ld_print_tree(+Triples:ordset(rdf_triple)) is det.

ld_print_triples(Triples1) :-
  triples_collections(Triples1, BLs, Triples2),
  triples_structure(Triples2, BPOs, SPOs),
  ld_print_subjects(BLs, BPOs, SPOs).


% This is where we extract collections.

triples_collections(Triples1, BLs, Triples2) :-
  partition(rdf_list_triple, Triples1, LTriples, Triples2),
  triples_collections_(LTriples, BLs).

rdf_list_triple(rdf(_,rdf:first,_)).
rdf_list_triple(rdf(_,rdf:next,_)).

triples_collections_(Triples1, [B-L|BLs]) :-
  root_collection_node(B, Triples1), !,
  triples_collections_(Triples1, B-L, Triples2),
  triples_collections_(Triples2, BLs).
triples_collections_(_, []).

root_collection_node(B, Triples) :-
  rdf_prefix_memberchk(rdf(B,rdf:first,_), Triples),
  \+ rdf_prefix_memberchk(rdf(_,rdf:next,B), Triples).

triples_collections_(Triples1, B1-[H|T], Triples4) :-
  rdf_prefix_selectchk(rdf(B1,rdf:first,H), Triples1, Triples2),
  rdf_prefix_selectchk(rdf(B1,rdf:next,B2), Triples2, Triples3), !,
  (   rdf_equal(rdf:nil, B2)
  ->  T = []
  ;   triples_collections_(Triples3, B2-T, Triples4)
  ).


% This is where we extract trees that begin with IRIs and trees that
% begin with blank nodes.

triples_structure([rdf(S,P,O)|T], BPOs, SPOs) :- !,
  subject_structure(S, [rdf(S,P,O)|T], POs, T_),
  (   rdf_is_skip_node(S)
  ->  BPOs = [S-POs|BPOs_],
      SPOs = SPOs_
  ;   BPOs = BPOs_,
      SPOs = [S-POs|SPOs_]
  ),
  triples_structure(T_, BPOs_, SPOs_).
triples_structure([], [], []).

subject_structure(S, [rdf(S,P,O)|T1], [P-Os|POs], L3) :- !,
  predicate_structure(S, P, [rdf(S,P,O)|T1], Os, T2),
  subject_structure(S, T2, POs, L3).
subject_structure(_, L, [], L).

predicate_structure(S, P, [rdf(S,P,O)|T1], [O|Os], L3) :- !,
  predicate_structure(S, P, T1, Os, L3).
predicate_structure(_, _, L, [], L).


% This is where we use the structure to print.

ld_print_subjects(BLs, BPOs, [S-POs|SPOs]) :- !,
  ld_print_subject(BLs, BPOs, S-POs),
  ld_print_subjects(BLs, BPOs, SPOs).
ld_print_subjects(_, _, []).

% one triple: all in one line
ld_print_subject(_, _, S-[P-[O]]) :- !,
  ld_print_nonliteral(S),
  format(" "),
  ld_print_iri(P),
  format(" "),
  ld_print_literal(O),
  format(".~n").
ld_print_subject(BLs, BPOs, S-POs) :-
  ld_print_nonliteral(S),
  nl,
  ld_print_predicates(4, BLs, BPOs, POs).

% the last predicate: end with a dot
ld_print_predicates(N, BLs, BPOs, [P-Os]) :- !,
  ld_print_predicate(N, BLs, BPOs, P-Os),
  % Do not emit an end-of-statement dot in nested blank nodes.
  (N > 4 -> true ; format(".")),
  nl.
% not the last predicate: end with a semi-colon
ld_print_predicates(N, BLs, BPOs, [P-Os|POs]) :-
  ld_print_predicate(N, BLs, BPOs, P-Os),
  format(";~n"),
  ld_print_predicates(N, BLs, BPOs, POs).

% one object: all in one line
ld_print_predicate(N, BLs, BPOs, P-[O]) :- !,
  tab(N),
  ld_print_predicate(P),
  format(" "),
  ld_print_subtree(N, BLs, BPOs, O).
ld_print_predicate(N1, BLs, BPOs, P-Os) :-
  tab(N1),
  ld_print_predicate(P),
  nl,
  N2 is N1 + 4,
  ld_print_objects(N2, BLs, BPOs, Os).

% the last object: determine the EOL symbol at the predicate level
ld_print_objects(N, BLs, BPOs, [O]) :- !,
  tab(N),
  ld_print_subtree(N, BLs, BPOs, O).
% not the last object: end with a comma
ld_print_objects(N, BLs, BPOs, [O|Os]) :-
  tab(N),
  ld_print_subtree(N, BLs, BPOs, O),
  format(",~n"),
  ld_print_objects(N, BLs, BPOs, Os).

% collection
ld_print_subtree(N1, BLs, BPOs, B) :-
  memberchk(B-L, BLs), !,
  (   L = [X]
  ->  format("( "),
      ld_print_subtree(N1, BLs, BPOs, X),
      format(" )")
  ;   format("(~n"),
      N2 is N1 + 4,
      maplist(ld_print_subtree(N2, BLs, BPOs), L),
      format(")")
  ).
% blank node nesting
ld_print_subtree(N1, BLs, BPOs, B) :-
  memberchk(B-POs, BPOs), !,
  (   POs = [P-[O]]
  ->  format("[ "),
      ld_print_iri(P),
      format(" "),
      ld_print_subtree(N1, BLs, BPOs, O),
      format(" ]")
  ;   format("[~n"),
      N2 is N1 + 4,
      ld_print_predicates(N2, BLs, BPOs, POs),
      tab(N1),
      format("]")
  ).
ld_print_subtree(_, _, _, Term) :-
  ld_print_term(Term).


% This is where we print the atomic terms.

ld_print_iri(Iri) :-
  rdf_prefix_iri(Alias, Local, Iri), !,
  format("~a:~a", [Alias,Local]).
ld_print_iri(Iri) :-
  format("<~a>", [Iri]).

% abbreviated notation for `xsd:boolean'
ld_print_literal(literal(type(xsd:boolean,Lex))) :- !,
  format("~a", [Lex]).
% abbreviated notation for `xsd:decimal', `xsd:double', and
% `xsd:integer'
ld_print_literal(literal(type(D,Lex))) :-
  rdf_prefix_memberchk(D, [xsd:decimal,xsd:double,xsd:integer]), !,
  format("~a", [Lex]).
% abbreviated notation for `xsd:string'
ld_print_literal(literal(type(xsd:string,Lex))) :- !,
  format("\"~a\"", [Lex]).
% non-abbreviated typed literal
ld_print_literal(literal(type(D,Lex))) :- !,
  format("\"~a\"^^", [Lex]),
  ld_print_iri(D).
% language-tagged string
ld_print_literal(literal(lang(LTag,Lex))) :-
  format("\"~a\"@~a", [Lex,LTag]).

ld_print_nonliteral(Iri) :-
  rdf_is_iri(Iri), !,
  ld_print_iri(Iri).
ld_print_nonliteral(BNode) :-
  format("~a", [BNode]).

ld_print_predicate(rdf:type) :- !,
  format("a").
ld_print_predicate(Iri) :-
  ld_print_iri(Iri).

ld_print_term(Literal) :-
  rdf_is_literal(Literal), !,
  ld_print_literal(Literal).
ld_print_term(NonLiteral) :-
  ld_print_nonliteral(NonLiteral).
