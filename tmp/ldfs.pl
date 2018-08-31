%! ldfs_next(+Hash1:atom, -Hash2:atom) is nondet.

ldfs_next(Hash1, Hash2) :-
  ground(Hash1), !,
  ldfs(meta, _, ll:entry, Entry, Hash1),
  rdf_prefix_iri(graph:Hash2, Entry).
ldfs_next(Hash1, Hash2) :-
  ground(Hash2), !,
  ldfs(meta, Archive, ll:entry, _, Hash2),
  rdf_prefix_iri(graph:Hash1, Archive).
ldfs_next(Hash1, Hash2) :-
  instantiation_error(args([Hash1,Hash2])).
